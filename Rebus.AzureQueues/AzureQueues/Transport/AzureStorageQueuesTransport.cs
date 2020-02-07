using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.Storage.RetryPolicies;
using Newtonsoft.Json;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Time;
using Rebus.Transport;
// ReSharper disable MethodSupportsCancellation
// ReSharper disable EmptyGeneralCatchClause
#pragma warning disable 1998

namespace Rebus.AzureQueues.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses Azure Storage Queues to do its thing
    /// </summary>
    public class AzureStorageQueuesTransport : ITransport, IInitializable
    {
        const string QueueNameValidationRegex = "^[a-z0-9](?!.*--)[a-z0-9-]{1,61}[a-z0-9]$";
        static readonly QueueRequestOptions ExponentialRetryRequestOptions = new QueueRequestOptions { RetryPolicy = new ExponentialRetry() };
        static readonly QueueRequestOptions DefaultQueueRequestOptions = new QueueRequestOptions();
        readonly AzureStorageQueuesTransportOptions _options;
        readonly ConcurrentDictionary<string, CloudQueue> _queues = new ConcurrentDictionary<string, CloudQueue>();
        readonly ConcurrentQueue<CloudQueueMessage> _prefetchedMessages = new ConcurrentQueue<CloudQueueMessage>();
        readonly TimeSpan _initialVisibilityDelay;
        readonly ILog _log;
        readonly ICloudQueueFactory _queueFactory;
        readonly IRebusTime _rebusTime;

        /// <summary>
        /// Constructs the transport using a <see cref="CloudStorageAccount"/>
        /// </summary>
        public AzureStorageQueuesTransport(CloudStorageAccount storageAccount, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, AzureStorageQueuesTransportOptions options, IRebusTime rebusTime)
        {
            if (storageAccount == null) throw new ArgumentNullException(nameof(storageAccount));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _options = options;
            _rebusTime = rebusTime;
            _log = rebusLoggerFactory.GetLogger<AzureStorageQueuesTransport>();
            _initialVisibilityDelay = options.InitialVisibilityDelay;

            var queueClient = storageAccount.CreateCloudQueueClient();
            _queueFactory = new CloudQueueClientQueueFactory(queueClient);

            if (inputQueueName != null)
            {
                if (!Regex.IsMatch(inputQueueName, QueueNameValidationRegex))
                {
                    throw new ArgumentException($"The inputQueueName {inputQueueName} is not valid - it can contain only alphanumeric characters and hyphens, and must not have 2 consecutive hyphens.", nameof(inputQueueName));
                }
                Address = inputQueueName.ToLowerInvariant();
            }
        }
        
        /// <summary>
        /// Constructs the transport using a <see cref="ICloudQueueFactory"/>
        /// </summary>
        public AzureStorageQueuesTransport(ICloudQueueFactory queueFactory, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, AzureStorageQueuesTransportOptions options, IRebusTime rebusTime)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            _options = options ?? throw new ArgumentNullException(nameof(options));
            _rebusTime = rebusTime;
            _log = rebusLoggerFactory.GetLogger<AzureStorageQueuesTransport>();
            _initialVisibilityDelay = options.InitialVisibilityDelay;
            _queueFactory = queueFactory ?? throw new ArgumentNullException(nameof(queueFactory));

            if (inputQueueName != null)
            {
                if (!Regex.IsMatch(inputQueueName, QueueNameValidationRegex))
                {
                    throw new ArgumentException($"The inputQueueName {inputQueueName} is not valid - it can contain only alphanumeric characters and hyphens, and must not have 2 consecutive hyphens.", nameof(inputQueueName));
                }
                Address = inputQueueName.ToLowerInvariant();
            }
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
                var queue = await _queueFactory.GetQueue(address);
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

                context.OnCommitted((_) =>
                {
                    var messagesByQueue = messagesToSend
                        .GroupBy(m => m.DestinationAddress)
                        .ToList();

                    return Task.WhenAll(messagesByQueue.Select(async batch =>
                    {
                        var queueName = batch.Key;
                        var queue = await _queueFactory.GetQueue(queueName);

                        await Task.WhenAll(batch.Select(async message =>
                        {
                            var headers = message.Headers.Clone();
                            var timeToBeReceivedOrNull = GetTimeToBeReceivedOrNull(headers);
                            var queueVisibilityDelayOrNull = GetQueueVisibilityDelayOrNull(headers);
                            var cloudQueueMessage = Serialize(headers, message.Body);

                            try
                            {
                                await queue.AddMessageAsync(
                                    cloudQueueMessage,
                                    timeToBeReceivedOrNull,
                                    queueVisibilityDelayOrNull,
                                    ExponentialRetryRequestOptions,
                                    new OperationContext()
                                );
                            }
                            catch (Exception exception)
                            {
                                var errorText = $"Could not send message with ID {cloudQueueMessage.Id} to '{message.DestinationAddress}'";

                                throw new RebusApplicationException(exception, errorText);
                            }
                        }));
                    }));
                });

                return messagesToSend;
            });

            var messageToSend = new MessageToSend(destinationAddress, transportMessage.Headers, transportMessage.Body);

            outgoingMessages.Enqueue(messageToSend);
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
                throw new InvalidOperationException("This Azure Storage Queues transport does not have an input queue, hence it is not possible to receive anything");
            }

            var inputQueue = await _queueFactory.GetQueue(Address);

            if (_prefetchedMessages.TryDequeue(out var dequeuedMessage))
            {
                SetUpCompletion(context, dequeuedMessage, inputQueue);
                return Deserialize(dequeuedMessage);
            }

            if (_options.Prefetch.HasValue)
            {
                var cloudQueueMessages = await inputQueue.GetMessagesAsync(
                    _options.Prefetch.Value,
                    _initialVisibilityDelay,
                    DefaultQueueRequestOptions,
                    new OperationContext(),
                    cancellationToken
                );

                foreach (var message in cloudQueueMessages)
                {
                    _prefetchedMessages.Enqueue(message);
                }

                if (_prefetchedMessages.TryDequeue(out var newlyPrefetchedMessage))
                {
                    SetUpCompletion(context, newlyPrefetchedMessage, inputQueue);
                    return Deserialize(newlyPrefetchedMessage);
                }

                return null;
            }

            var cloudQueueMessage = await inputQueue.GetMessageAsync(
                _initialVisibilityDelay,
                DefaultQueueRequestOptions,
                new OperationContext(),
                cancellationToken
            );

            if (cloudQueueMessage == null) return null;

            SetUpCompletion(context, cloudQueueMessage, inputQueue);
            return Deserialize(cloudQueueMessage);
        }

        static void SetUpCompletion(ITransactionContext context, CloudQueueMessage cloudQueueMessage, CloudQueue inputQueue)
        {
            var messageId = cloudQueueMessage.Id;
            var popReceipt = cloudQueueMessage.PopReceipt;

            context.OnCompleted(async (_) =>
            {
                try
                {
                    // if we get this far, don't pass on the cancellation token
                    // ReSharper disable once MethodSupportsCancellation
                    await inputQueue.DeleteMessageAsync(
                        messageId,
                        popReceipt,
                        ExponentialRetryRequestOptions,
                        new OperationContext()
                    );
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception, $"Could not delete message with ID {messageId} and pop receipt {popReceipt} from the input queue");
                }
            });

            context.OnAborted((_) =>
            {
                const MessageUpdateFields fields = MessageUpdateFields.Visibility;
                var visibilityTimeout = TimeSpan.FromSeconds(0);

                AsyncHelpers.RunSync(async () =>
                {
                    // ignore if this fails
                    try
                    {
                        await inputQueue.UpdateMessageAsync(cloudQueueMessage, visibilityTimeout, fields);
                    }
                    catch { }
                });
            });
        }

        static TimeSpan? GetTimeToBeReceivedOrNull(Dictionary<string, string> headers)
        {
            if (!headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedStr))
            {
                return null;
            }

            TimeSpan? timeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);

            return timeToBeReceived;
        }

        TimeSpan? GetQueueVisibilityDelayOrNull(Dictionary<string, string> headers)
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

        static CloudQueueMessage Serialize(Dictionary<string, string> headers, byte[] body)
        {
            var cloudStorageQueueTransportMessage = new CloudStorageQueueTransportMessage
            {
                Headers = headers,
                Body = body
            };

            return new CloudQueueMessage(JsonConvert.SerializeObject(cloudStorageQueueTransportMessage));
        }

        static TransportMessage Deserialize(CloudQueueMessage cloudQueueMessage)
        {
            var cloudStorageQueueTransportMessage = JsonConvert.DeserializeObject<CloudStorageQueueTransportMessage>(cloudQueueMessage.AsString);

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
                _log.Info("Initializing Azure Storage Queues transport with queue '{0}'", Address);
                CreateQueue(Address);
                return;
            }

            _log.Info("Initializing one-way Azure Storage Queues transport");
        }
        
        /// <summary>
        /// Purges the input queue (WARNING: potentially very slow operation, as it will continue to batch receive messages until the queue is empty
        /// </summary>
        /// <exception cref="RebusApplicationException"></exception>
        public void PurgeInputQueue()
        {
            AsyncHelpers.RunSync(async () =>
            {
                var queue = await _queueFactory.GetQueue(Address);

                if (!await queue.ExistsAsync()) return;

                _log.Info("Purging storage queue '{0}' (purging by deleting all messages)", Address);

                try
                {
                    await queue.ClearAsync(ExponentialRetryRequestOptions, new OperationContext());
                }
                catch (Exception exception)
                {
                    throw new RebusApplicationException(exception, "Could not purge queue");
                }
            });
        }
    }
}
