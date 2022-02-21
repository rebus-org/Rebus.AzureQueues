using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Queue;
#pragma warning disable CS1998

namespace Rebus.AzureQueues;

internal class CloudQueueClientQueueFactory : ICloudQueueFactory
{
    readonly ConcurrentDictionary<string, Task<CloudQueue>> _queues = new();
    readonly CloudQueueClient _cloudQueueClient;

    public CloudQueueClientQueueFactory(CloudQueueClient cloudQueueClient)
    {
        _cloudQueueClient = cloudQueueClient ?? throw new ArgumentNullException(nameof(cloudQueueClient));
    }

    public Task<CloudQueue> GetQueue(string queueName)
    {
        return _queues.GetOrAdd(queueName, async _ => _cloudQueueClient.GetQueueReference(queueName));
    }
}