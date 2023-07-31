using Azure.Storage.Queues;
using System.Threading.Tasks;
using System;
using Azure.Core;

#pragma warning disable CS1998

namespace Rebus.AzureQueues.Internals;

class AzureTokenCredentialQueueClientFactory : IQueueClientFactory
{
    private readonly TokenCredential _tokenCredential;
    private readonly Uri _serviceEndpoint;

    public AzureTokenCredentialQueueClientFactory(TokenCredential tokenCredential, Uri serviceEndpoint)
    {
        _tokenCredential = tokenCredential ?? throw new ArgumentNullException(nameof(tokenCredential));
        _serviceEndpoint = serviceEndpoint ?? throw new ArgumentNullException(nameof(serviceEndpoint));
    }

    public async Task<QueueClient> GetQueue(string queueName)
    {
        if (queueName == null) throw new ArgumentNullException(nameof(queueName));

        var uri = new Uri(_serviceEndpoint, queueName);

        return new QueueClient(uri, _tokenCredential);
    }
}