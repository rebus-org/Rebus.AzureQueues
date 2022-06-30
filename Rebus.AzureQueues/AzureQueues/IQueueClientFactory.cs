using System.Threading.Tasks;
using Azure.Storage.Queues;

namespace Rebus.AzureQueues;

/// <summary>
/// An abstraction to provide instances of <see cref="QueueClient"/>
/// </summary>
public interface IQueueClientFactory
{
    /// <summary>
    /// Retrieve an instance of <see cref="QueueClient"/> targeting the given queue
    /// </summary>
    /// <param name="queueName">The queue to retrieve a <see cref="QueueClient"/> for</param>
    Task<QueueClient> GetQueue(string queueName);
}