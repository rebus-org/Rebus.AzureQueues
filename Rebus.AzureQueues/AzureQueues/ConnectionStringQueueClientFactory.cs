using System;
using System.Threading.Tasks;
using Azure.Storage.Queues;

#pragma warning disable CS1998

namespace Rebus.AzureQueues;

public class ConnectionStringQueueClientFactory : IQueueClientFactory
{
    readonly string _connectionString;

    public ConnectionStringQueueClientFactory(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task<QueueClient> GetQueue(string queueName)
    {
        return new QueueClient(_connectionString, queueName);
    }
}