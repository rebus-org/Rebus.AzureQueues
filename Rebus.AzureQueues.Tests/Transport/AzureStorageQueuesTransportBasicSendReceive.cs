using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;

#pragma warning disable 1998

namespace Rebus.AzureQueues.Tests.Transport;

[TestFixture]
public class AzureStorageQueuesTransportBasicSendReceive : BasicSendReceive<AzureStorageQueuesTransportFactory>
{
    protected override TransportBehavior Behavior => new(ReturnsNullWhenQueueIsEmpty: true);
}