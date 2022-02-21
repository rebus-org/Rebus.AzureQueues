using System.Threading.Tasks;
using Microsoft.Azure.Storage.Queue;

namespace Rebus.AzureQueues;

/// <summary>
/// An abstraction to provide instances of <see cref="CloudQueue"/>
/// </summary>
public interface ICloudQueueFactory
{
    /// <summary>
    /// Retrieve an instance of <see cref="CloudQueue"/> targeting the given queue
    /// </summary>
    /// <param name="queueName">The queue to retrieve a <see cref="CloudQueue"/> for</param>
    /// <returns></returns>
    Task<CloudQueue> GetQueue(string queueName);
}