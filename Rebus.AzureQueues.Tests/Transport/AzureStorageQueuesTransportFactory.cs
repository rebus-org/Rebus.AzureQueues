using System;
using System.Collections.Concurrent;
using Rebus.AzureQueues.Transport;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts.Transports;
using Rebus.Transport;

namespace Rebus.AzureStorage.Tests.Transport
{
    public class AzureStorageQueuesTransportFactory : ITransportFactory
    {
        readonly ConcurrentDictionary<string, AzureStorageQueuesTransport> _transports = new ConcurrentDictionary<string, AzureStorageQueuesTransport>(StringComparer.OrdinalIgnoreCase);

        public ITransport CreateOneWayClient()
        {
            return Create(null);
        }

        public ITransport Create(string inputQueueAddress)
        {
            if (inputQueueAddress == null)
            {
                var transport = new AzureStorageQueuesTransport(AzureConfig.StorageAccount, null, new ConsoleLoggerFactory(false), new AzureStorageQueuesTransportOptions());

                transport.Initialize();

                return transport;
            }

            return _transports.GetOrAdd(inputQueueAddress, address =>
            {
                var transport = new AzureStorageQueuesTransport(AzureConfig.StorageAccount, inputQueueAddress, new ConsoleLoggerFactory(false), new AzureStorageQueuesTransportOptions());

                transport.PurgeInputQueue();

                transport.Initialize();

                return transport;
            });
        }

        public void CleanUp()
        {
        }
    }
}
