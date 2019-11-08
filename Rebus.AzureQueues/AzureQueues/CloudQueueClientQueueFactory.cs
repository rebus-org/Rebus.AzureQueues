using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Rebus.AzureQueues
{
    internal class CloudQueueClientQueueFactory : ICloudQueueFactory
    {
        private readonly ConcurrentDictionary<string, Task<CloudQueue>> _queues = new ConcurrentDictionary<string, Task<CloudQueue>>();
        private readonly CloudQueueClient _cloudQueueClient;

        public CloudQueueClientQueueFactory(CloudQueueClient cloudQueueClient)
        {
            _cloudQueueClient = cloudQueueClient;
        }

        private Task<CloudQueue> InternalQueueFactory(string queueName)
        {
            return Task.FromResult(_cloudQueueClient.GetQueueReference(queueName));
        }

        public Task<CloudQueue> GetQueue(string queueName)
        {
            return _queues.GetOrAdd(queueName, InternalQueueFactory);
        }
    }
}
