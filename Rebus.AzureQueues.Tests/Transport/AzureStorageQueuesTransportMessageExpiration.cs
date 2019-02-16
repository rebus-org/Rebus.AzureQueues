using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.AzureStorage.Tests.Transport
{
    [TestFixture]
    public class AzureStorageQueuesTransportMessageExpiration : MessageExpiration<AzureStorageQueuesTransportFactory>
    {
    }
}