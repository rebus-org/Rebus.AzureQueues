using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Rebus.AzureQueues
{
    public interface ICloudQueueFactory
    {
        Task<CloudQueue> GetQueue(string queueName);
    }
}
