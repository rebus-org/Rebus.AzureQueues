using System;
using Azure.Storage.Queues;

namespace Rebus.AzureQueues.Tests;

class QueueDeleter : IDisposable
{
    private readonly string _queueName;

    public QueueDeleter(string queueName) => _queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));

    public void Dispose() => new QueueClient(AzureConfig.ConnectionString, _queueName).DeleteIfExists();
}