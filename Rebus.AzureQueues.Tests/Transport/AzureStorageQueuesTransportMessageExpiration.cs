using NUnit.Framework;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.AzureQueues.Tests.Transport;

[TestFixture]
public class AzureStorageQueuesTransportMessageExpiration : MessageExpiration<AzureStorageQueuesTransportFactory>
{
}