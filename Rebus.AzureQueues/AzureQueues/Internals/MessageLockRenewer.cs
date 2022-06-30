using System;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;

namespace Rebus.AzureQueues.Internals;

class MessageLockRenewer
{
    readonly QueueClient _queueClient;
    readonly QueueMessage _message;

    DateTimeOffset _nextRenewal;
    DateTimeOffset? _nextVisibleOn;

    public MessageLockRenewer(QueueMessage message, QueueClient queueClient)
    {
        _message = message ?? throw new ArgumentNullException(nameof(message));
        _queueClient = queueClient ?? throw new ArgumentNullException(nameof(queueClient));
    
        MessageId = message.MessageId;
        PopReceipt = message.PopReceipt;
        _nextVisibleOn = message.NextVisibleOn;

        _nextRenewal = GetTimeOfNextRenewal();
    }

    public string MessageId { get; }
    public string PopReceipt { get; private set; }

    public bool IsDue => DateTimeOffset.Now >= _nextRenewal;

    public async Task Renew()
    {
        // intentionally let exceptions bubble out here, so the caller can log it as a warning
        var response = await _queueClient.UpdateMessageAsync(MessageId, PopReceipt, visibilityTimeout: TimeSpan.FromMinutes(5));

        PopReceipt = response.Value.PopReceipt;
        _nextVisibleOn = response.Value.NextVisibleOn;

        _nextRenewal = GetTimeOfNextRenewal();
    }

    DateTimeOffset GetTimeOfNextRenewal()
    {
        var remainingTime = LockedUntil - DateTimeOffset.Now;
        var halfOfRemainingTime = TimeSpan.FromMinutes(0.5 * remainingTime.TotalMinutes);

        return DateTimeOffset.Now + halfOfRemainingTime;
    }

    DateTimeOffset LockedUntil => _nextVisibleOn ?? DateTimeOffset.MinValue;
}