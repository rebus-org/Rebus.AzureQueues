using System;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
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
            var storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);

            Register(configurer, null, storageAccount, options);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages
        /// </summary>
        public static void UseAzureStorageQueues(this StandardConfigurer<ITransport> configurer, string storageAccountConnectionString, string inputQueueAddress, AzureStorageQueuesTransportOptions options = null)
        {
            var storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);

            Register(configurer, inputQueueAddress, storageAccount, options);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages as a one-way client (i.e. will not be able to receive any messages)
        /// </summary>
        public static void UseAzureStorageQueuesAsOneWayClient(this StandardConfigurer<ITransport> configurer, CloudStorageAccount storageAccount, AzureStorageQueuesTransportOptions options = null)
        {
            Register(configurer, null, storageAccount, options);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages
        /// </summary>
        public static void UseAzureStorageQueues(this StandardConfigurer<ITransport> configurer, ICloudQueueFactory queueFactory, string inputQueueAddress, AzureStorageQueuesTransportOptions options = null)
        {
            Register(configurer, inputQueueAddress, queueFactory, options);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages as a one-way client (i.e. will not be able to receive any messages)
        /// </summary>
        public static void UseAzureStorageQueuesAsOneWayClient(this StandardConfigurer<ITransport> configurer, ICloudQueueFactory queueFactory, AzureStorageQueuesTransportOptions options = null)
        {
            Register(configurer, null, queueFactory, options);

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }

        /// <summary>
        /// Configures Rebus to use Azure Storage Queues to transport messages
        /// </summary>
        public static void UseAzureStorageQueues(this StandardConfigurer<ITransport> configurer, CloudStorageAccount storageAccount, string inputQueueAddress, AzureStorageQueuesTransportOptions options = null)
        {
            Register(configurer, inputQueueAddress, storageAccount, options);
        }


        static void Register(StandardConfigurer<ITransport> configurer, string inputQueueAddress,
            CloudStorageAccount cloudStorageAccount, AzureStorageQueuesTransportOptions optionsOrNull)
        {
            var queueFactory = new CloudQueueClientQueueFactory(cloudStorageAccount.CreateCloudQueueClient());
            Register(configurer, inputQueueAddress, queueFactory, optionsOrNull);
        }

        static void Register(StandardConfigurer<ITransport> configurer, string inputQueueAddress, ICloudQueueFactory queueFactory, AzureStorageQueuesTransportOptions optionsOrNull)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (queueFactory == null) throw new ArgumentNullException(nameof(queueFactory));

            var options = optionsOrNull ?? new AzureStorageQueuesTransportOptions();

            configurer.Register(c =>
            {
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var rebusTime = c.Get<IRebusTime>();
                return new AzureStorageQueuesTransport(queueFactory, inputQueueAddress, rebusLoggerFactory, options, rebusTime, asyncTaskFactory);
            });

            if (options.UseNativeDeferredMessages)
            {
                configurer.OtherService<ITimeoutManager>().Register(c => new DisabledTimeoutManager(), description: AsqTimeoutManagerText);

                configurer.OtherService<IPipeline>().Decorate(c =>
                {
                    var pipeline = c.Get<IPipeline>();

                    return new PipelineStepRemover(pipeline)
                        .RemoveIncomingStep(s => s.GetType() == typeof(HandleDeferredMessagesStep));
                });
            }
        }
    }
}