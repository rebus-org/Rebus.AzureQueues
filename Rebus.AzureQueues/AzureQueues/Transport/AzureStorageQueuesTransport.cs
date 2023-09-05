using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Rebus.AzureQueues.Internals;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;
// ReSharper disable MethodSupportsCancellation
// ReSharper disable EmptyGeneralCatchClause
// ReSharper disable UnusedMember.Global
#pragma warning disable 1998

namespace Rebus.AzureQueues.Transport;

/// <summary>
/// Implementation of <see cref="ITransport"/> that uses Azure Storage Queues to do its thing
/// </summary>
public class AzureStorageQueuesTransport : ITransport, IInitializable, IDisposable
{
    /// <summary>
    /// External timeout manager address set to this magic address will be routed to the destination address specified by the <see cref="Headers.DeferredRecipient"/> header
    /// </summary>
    public const string MagicDeferredMessagesAddress = "___deferred___";

    const string QueueNameValidationRegex = "^[a-z0-9](?!.*--)[a-z0-9-]{1,61}[a-z0-9]$";
    readonly ConcurrentDictionary<string, MessageLockRenewer> _messageLockRenewers = new();
    readonly ConcurrentQueue<QueueMessage> _prefetchedMessages = new();
    readonly AzureStorageQueuesTransportOptions _options;
    readonly IAsyncTask _messageLockRenewalTask;
    readonly TimeSpan _initialVisibilityDelay;
    readonly ILog _log;
    readonly IQueueClientFactory _queueClientFactory;
    readonly IRebusTime _rebusTime;

    /// <summary>
    /// Constructs the transport using a <see cref="IQueueClientFactory"/>
    /// </summary>
    public AzureStorageQueuesTransport(IQueueClientFactory queueClientFactory, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, AzureStorageQueuesTransportOptions options, IRebusTime rebusTime, IAsyncTaskFactory asyncTaskFactory)
    {
        if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

        _options = options ?? throw new ArgumentNullException(nameof(options));
        _rebusTime = rebusTime;
        _log = rebusLoggerFactory.GetLogger<AzureStorageQueuesTransport>();
        _initialVisibilityDelay = options.InitialVisibilityDelay;
        _queueClientFactory = queueClientFactory ?? throw new ArgumentNullException(nameof(queueClientFactory));

        if (inputQueueName != null)
        {
            if (!Regex.IsMatch(inputQueueName, QueueNameValidationRegex))
            {
                throw new ArgumentException($"The inputQueueName {inputQueueName} is not valid - it can contain only alphanumeric characters and hyphens, and must not have 2 consecutive hyphens.", nameof(inputQueueName));
            }
            Address = inputQueueName.ToLowerInvariant();
        }
        _messageLockRenewalTask = asyncTaskFactory.Create("Peek Lock Renewal", RenewPeekLocks, prettyInsignificant: true, intervalSeconds: 10);
    }

    /// <summary>
    /// Creates a new queue with the specified address
    /// </summary>
    public void CreateQueue(string address)
    {
        if (!_options.AutomaticallyCreateQueues)
        {
            _log.Info("Transport configured to not create queue - skipping existence check and potential creation for {queueName}", address);
            return;
        }

        AsyncHelpers.RunSync(async () =>
        {
            var queue = await _queueClientFactory.GetQueue(address);
            await queue.CreateIfNotExistsAsync();
        });
    }

    /// <summary>
    /// Sends the given <see cref="TransportMessage"/> to the queue with the specified globally addressable name
    /// </summary>
    public async Task Send(string destinationAddress, TransportMessage transportMessage, ITransactionContext context)
    {
        var outgoingMessages = context.GetOrAdd("outgoing-messages", () =>
        {
            var messagesToSend = new ConcurrentQueue<MessageToSend>();

            context.OnCommit(_ =>
            {
                var messagesByQueue = messagesToSend
                    .GroupBy(m => m.DestinationAddress)
                    .ToList();

                return Task.WhenAll(messagesByQueue.Select(async batch =>
                {
                    var queueName = batch.Key;
                    var queue = await _queueClientFactory.GetQueue(queueName);

                    await Task.WhenAll(batch.Select(async message =>
                    {
                        var headers = message.Headers.Clone();
                        var timeToLiveOrNull = GetTimeToBeReceivedOrNull(headers);
                        var visibilityDelayOrNull = GetQueueVisibilityDelayOrNull(headers);
                        var cloudQueueMessage = Serialize(headers, message.Body);

                        try
                        {
                            await queue.SendMessageAsync(
                                message: cloudQueueMessage,
                                timeToLive: timeToLiveOrNull,
                                visibilityTimeout: visibilityDelayOrNull
                            );
                        }
                        catch (Exception exception)
                        {
                            var errorText = $"Could not send message with ID {transportMessage.GetMessageId()} to '{message.DestinationAddress}'";

                            throw new RebusApplicationException(exception, errorText);
                        }
                    }));
                }));
            });

            return messagesToSend;
        });

        var messageToSend = GetMessageToSend(destinationAddress, transportMessage);

        outgoingMessages.Enqueue(messageToSend);
    }

    static MessageToSend GetMessageToSend(string destinationAddress, TransportMessage transportMessage)
    {
        var headers = transportMessage.Headers;

        if (destinationAddress == MagicDeferredMessagesAddress)
        {
            var actualDestinationAddress = headers.GetValueOrNull(Headers.DeferredRecipient)
                                           ?? throw new ArgumentException($"Message was deferred, but the '{Headers.DeferredRecipient}' header could not be found. When a message is sent 'into the future', the '{Headers.DeferredRecipient}' header must indicate which queue to deliver the message after the delay has passed, and the '{Headers.DeferredUntil}' header must indicate the earliest time of when the message must be delivered.");

            if (!headers.ContainsKey(Headers.DeferredUntil))
            {
                throw new ArgumentException(
                    $"Message was deferred, but the '{Headers.DeferredUntil}' header could not be found. When a message is sent 'into the future', the '{Headers.DeferredRecipient}' header must indicate which queue to deliver the message after the delay has passed, and the '{Headers.DeferredUntil}' header must indicate the earliest time of when the message must be delivered.");
            }

            var clone = headers.Clone();
            
            clone.Remove(Headers.DeferredRecipient);

            return new MessageToSend(actualDestinationAddress, clone, transportMessage.Body);
        }

        return new MessageToSend(destinationAddress, headers, transportMessage.Body);
    }

    class MessageToSend
    {
        public string DestinationAddress { get; }
        public Dictionary<string, string> Headers { get; }
        public byte[] Body { get; }

        public MessageToSend(string destinationAddress, Dictionary<string, string> headers, byte[] body)
        {
            DestinationAddress = destinationAddress;
            Headers = headers;
            Body = body;
        }
    }

    /// <summary>
    /// Receives the next message (if any) from the transport's input queue <see cref="ITransport.Address"/>
    /// </summary>
    public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        if (Address == null)
        {
            throw new InvalidOperationException("This Azure Storage Queues transport does not have an input queue, which means that it is configured to be a one-way client. Therefore, it is not possible to receive anything.");
        }

        var inputQueue = await _queueClientFactory.GetQueue(Address);

        return await InnerReceive(context, cancellationToken, inputQueue);
    }

    async Task<TransportMessage> InnerReceive(ITransactionContext context, CancellationToken cancellationToken, QueueClient inputQueue)
    {
        if (!_options.Prefetch.HasValue)
        {
            // fetch single message
            var response = await inputQueue.ReceiveMessageAsync(
                visibilityTimeout: _initialVisibilityDelay,
                cancellationToken: cancellationToken
            );

            var cloudQueueMessage = response.Value;
            if (cloudQueueMessage == null) return null;

            if (_options.AutomaticPeekLockRenewalEnabled)
            {
                _messageLockRenewers.TryAdd(cloudQueueMessage.MessageId, new MessageLockRenewer(cloudQueueMessage, inputQueue));
            }

            SetUpCompletion(context, cloudQueueMessage, inputQueue);
            return Deserialize(cloudQueueMessage.Body);
        }

        if (_prefetchedMessages.TryDequeue(out var dequeuedMessage))
        {
            SetUpCompletion(context, dequeuedMessage, inputQueue);
            return Deserialize(dequeuedMessage.Body);
        }

        var batchResponse = await inputQueue.ReceiveMessagesAsync(
            maxMessages: _options.Prefetch.Value,
            visibilityTimeout: _initialVisibilityDelay,
            cancellationToken: cancellationToken
        );

        var cloudQueueMessages = batchResponse.Value;

        foreach (var message in cloudQueueMessages)
        {
            _prefetchedMessages.Enqueue(message);
        }

        if (_prefetchedMessages.TryDequeue(out var newlyPrefetchedMessage))
        {
            SetUpCompletion(context, newlyPrefetchedMessage, inputQueue);
            return Deserialize(newlyPrefetchedMessage.Body);
        }

        return null;
    }

    void SetUpCompletion(ITransactionContext context, QueueMessage cloudQueueMessage, QueueClient inputQueue)
    {
        var messageId = cloudQueueMessage.MessageId;

        context.OnAck(async _ =>
        {
            //if the message has been Automatic renewed, the popreceipt might have changed since setup
            var popReceipt = _messageLockRenewers.TryRemove(messageId, out var renewer)
                ? renewer.PopReceipt
                : cloudQueueMessage.PopReceipt;

            try
            {
                // if we get this far, don't pass on the cancellation token
                // ReSharper disable once MethodSupportsCancellation
                await inputQueue.DeleteMessageAsync(
                    messageId: messageId,
                    popReceipt: popReceipt
                );
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not delete message with ID {messageId} and pop receipt {popReceipt} from the input queue");
            }
        });

        context.OnNack(async _ =>
        {
            var visibilityTimeout = TimeSpan.FromSeconds(0);

            var popReceipt = _messageLockRenewers.TryRemove(messageId, out var renewer)
                ? renewer.PopReceipt
                : cloudQueueMessage.PopReceipt;

            // ignore if this fails
            try
            {
                await inputQueue.UpdateMessageAsync(cloudQueueMessage.MessageId, popReceipt, visibilityTimeout: visibilityTimeout);
            }
            catch { }
        });

        context.OnDisposed(ctx => _messageLockRenewers.TryRemove(messageId, out _));
    }

    static TimeSpan? GetTimeToBeReceivedOrNull(IReadOnlyDictionary<string, string> headers)
    {
        if (!headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedStr))
        {
            return null;
        }

        try
        {
            return TimeSpan.Parse(timeToBeReceivedStr);
        }
        catch (Exception exception)
        {
            throw new FormatException($"Could not parse the string '{timeToBeReceivedStr}' into a TimeSpan!", exception);
        }
    }

    TimeSpan? GetQueueVisibilityDelayOrNull(IDictionary<string, string> headers)
    {
        if (!_options.UseNativeDeferredMessages)
        {
            return null;
        }

        if (!headers.TryGetValue(Headers.DeferredUntil, out var deferredUntilDateTimeOffsetString))
        {
            return null;
        }

        headers.Remove(Headers.DeferredUntil);

        var enqueueTime = deferredUntilDateTimeOffsetString.ToDateTimeOffset();

        var difference = enqueueTime - _rebusTime.Now;
        if (difference <= TimeSpan.Zero) return null;
        return difference;
    }

    static BinaryData Serialize(Dictionary<string, string> headers, byte[] body)
    {
        var cloudStorageQueueTransportMessage = new CloudStorageQueueTransportMessage
        {
            Headers = headers,
            Body = body
        };

        return BinaryData.FromObjectAsJson(cloudStorageQueueTransportMessage);
    }

    static TransportMessage Deserialize(BinaryData cloudQueueMessage)
    {
        var cloudStorageQueueTransportMessage = cloudQueueMessage.ToObjectFromJson<CloudStorageQueueTransportMessage>();

        return new TransportMessage(cloudStorageQueueTransportMessage.Headers, cloudStorageQueueTransportMessage.Body);
    }

    class CloudStorageQueueTransportMessage
    {
        public Dictionary<string, string> Headers { get; set; }
        public byte[] Body { get; set; }
    }

    /// <inheritdoc />
    public string Address { get; }

    /// <summary>
    /// Initializes the transport by creating the input queue if necessary
    /// </summary>
    public void Initialize()
    {
        if (Address != null)
        {
            _log.Info("Initializing Azure Storage Queues transport with queue {queueName}", Address);
            CreateQueue(Address);
            if (_options.AutomaticPeekLockRenewalEnabled)
            {
                _messageLockRenewalTask.Start();
            }
            return;
        }
        _log.Info("Initializing one-way Azure Storage Queues transport");
    }

    /// <summary>
    /// Purges the input queue
    /// </summary>
    public void PurgeInputQueue()
    {
        AsyncHelpers.RunSync(async () =>
        {
            var queue = await _queueClientFactory.GetQueue(Address);

            if (!await queue.ExistsAsync()) return;

            _log.Info("Purging storage queue {queueName} (purging by deleting all messages)", Address);

            try
            {
                await queue.ClearMessagesAsync();
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, "Could not purge queue");
            }
        });
    }

    async Task RenewPeekLocks()
    {
        var mustBeRenewed = _messageLockRenewers.Values
            .Where(r => r.IsDue)
            .ToList();

        if (!mustBeRenewed.Any()) return;

        _log.Debug("Found {count} peek locks to be renewed", mustBeRenewed.Count);

        await Task.WhenAll(mustBeRenewed.Select(async r =>
        {
            try
            {
                await r.Renew().ConfigureAwait(false);

                _log.Debug("Successfully renewed peek lock for message with ID {messageId}", r.MessageId);
            }
            catch (Exception exception)
            {
                _log.Warn(exception, "Error when renewing peek lock for message with ID {messageId}", r.MessageId);
            }
        }));
    }

    /// <summary>
    /// Disposes background running tasks
    /// </summary>
    public void Dispose()
    {
        _messageLockRenewalTask?.Dispose();
    }
}