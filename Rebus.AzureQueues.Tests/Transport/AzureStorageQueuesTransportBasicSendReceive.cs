using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;
#pragma warning disable 1998

namespace Rebus.AzureStorage.Tests.Transport
{
    [TestFixture]
    public class AzureStorageQueuesTransportBasicSendReceive : BasicSendReceive<AzureStorageQueuesTransportFactory>
    {
    }
}