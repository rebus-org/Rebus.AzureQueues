using Microsoft.Azure.Storage.Queue;
using System;
using System.Threading.Tasks;

namespace Rebus.AzureQueues.Internals;

class MessageLockRenewer
{
    readonly CloudQueueMessage _message;
    readonly CloudQueue _messageReceiver;

    DateTimeOffset _nextRenewal;

    public MessageLockRenewer(CloudQueueMessage message, CloudQueue messageReceiver)
    {
        _message = message ?? throw new ArgumentNullException(nameof(message));
        _messageReceiver = messageReceiver ?? throw new ArgumentNullException(nameof(messageReceiver));
        _nextRenewal = GetTimeOfNextRenewal();
    }
    public string PopReceipt => _message.PopReceipt;
    public string MessageId => _message.Id;

    public bool IsDue => DateTimeOffset.Now >= _nextRenewal;

    public async Task Renew()
    {
        // intentionally let exceptions bubble out here, so the caller can log it as a warning
        await _messageReceiver.UpdateMessageAsync(_message, TimeSpan.FromMinutes(5), MessageUpdateFields.Visibility);

        _nextRenewal = GetTimeOfNextRenewal();
    }

    DateTimeOffset GetTimeOfNextRenewal()
    {
        var now = DateTimeOffset.Now;

        var remainingTime = LockedUntil - now;
        var halfOfRemainingTime = TimeSpan.FromMinutes(0.5 * remainingTime.TotalMinutes);

        return now + halfOfRemainingTime;
    }

    DateTimeOffset LockedUntil => _message.NextVisibleTime ?? DateTimeOffset.MinValue;
}