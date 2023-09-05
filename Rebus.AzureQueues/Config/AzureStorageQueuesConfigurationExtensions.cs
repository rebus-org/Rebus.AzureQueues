using System;
using Azure.Storage.Queues;
using Rebus.AzureQueues;
using Rebus.AzureQueues.Transport;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Timeouts;
using Rebus.Transport;
// ReSharper disable ArgumentsStyleNamedExpression
// ReSharper disable UnusedMember.Global

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the Aure Storage Queue transport
    /// </summary>
    public static class AzureStorageQueuesConfigurationExtensions
    {
        const string AsqTimeoutManagerText = @"A disabled timeout manager was installed as part of the Azure Storage Queues configuration, becuase the transport has native support for deferred messages.

If you don't want to use Azure Storage Queues' native support for deferred messages, please pass AzureStorageQueuesTransportOptions with UseNativeDeferredMessages = false when
configuring the transport, e.g. like so:

Configure.With(...)
    .Transport(t => {
        var options = new AzureStorageQueuesTransportOptions { UseNativeDeferredMessages = false };

        t.UseAzureStorageQueues(storageAccount, ""my-queue"", options: options);
    })
    .(...)
    .Start();";

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages as a one-way client (i.e. will not be able to receive any messages)
        /// </summary>
        public static void UseAzureStorageQueuesAsOneWayClient(this StandardConfigurer<ITransport> configurer, string storageAccountConnectionString, AzureStorageQueuesTransportOptions options = null)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (storageAccountConnectionString == null) throw new ArgumentNullException(nameof(storageAccountConnectionString));

            Register(configurer, null, new ConnectionStringQueueClientFactory(storageAccountConnectionString), options);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages
        /// </summary>
        public static void UseAzureStorageQueues(this StandardConfigurer<ITransport> configurer, string storageAccountConnectionString, string inputQueueAddress, AzureStorageQueuesTransportOptions options = null)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (storageAccountConnectionString == null) throw new ArgumentNullException(nameof(storageAccountConnectionString));
            if (inputQueueAddress == null) throw new ArgumentNullException(nameof(inputQueueAddress));

            Register(configurer, inputQueueAddress, new ConnectionStringQueueClientFactory(storageAccountConnectionString), options);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages, using the given <paramref name="queueClientFactory"/> to obtain the necessary <see cref="QueueClient"/> instances
        /// </summary>
        public static void UseAzureStorageQueues(this StandardConfigurer<ITransport> configurer, IQueueClientFactory queueClientFactory, string inputQueueAddress, AzureStorageQueuesTransportOptions options = null)
        {
            Register(configurer, inputQueueAddress, queueClientFactory, options);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages as a one-way client (i.e. will not be able to receive any messages), using the given <paramref name="queueClientFactory"/> to obtain the necessary <see cref="QueueClient"/> instances
        /// </summary>
        public static void UseAzureStorageQueuesAsOneWayClient(this StandardConfigurer<ITransport> configurer, IQueueClientFactory queueClientFactory, AzureStorageQueuesTransportOptions options = null)
        {
            Register(configurer, null, queueClientFactory, options);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        static void Register(StandardConfigurer<ITransport> configurer, string inputQueueAddress, IQueueClientFactory queueClientFactory, AzureStorageQueuesTransportOptions optionsOrNull)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (queueClientFactory == null) throw new ArgumentNullException(nameof(queueClientFactory));

            var options = optionsOrNull ?? new AzureStorageQueuesTransportOptions();

            configurer.Register(c =>
            {
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var rebusTime = c.Get<IRebusTime>();
                return new AzureStorageQueuesTransport(queueClientFactory, inputQueueAddress, rebusLoggerFactory, options, rebusTime, asyncTaskFactory);
            });

            if (options.UseNativeDeferredMessages)
            {
                configurer.OtherService<ITimeoutManager>().Register(_ => new DisabledTimeoutManager(), description: AsqTimeoutManagerText);

                configurer.OtherService<IPipeline>().Decorate(c =>
                {
                    var pipeline = c.Get<IPipeline>();

                    return new PipelineStepRemover(pipeline)
                        .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
                });

                configurer.OtherService<Options>().Decorate(c =>
                {
                    var rebusOptions = c.Get<Options>();
                    rebusOptions.ExternalTimeoutManagerAddressOrNull = AzureStorageQueuesTransport.MagicDeferredMessagesAddress;
                    return rebusOptions;
                });
            }
        }
    }
}