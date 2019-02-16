using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Persistence.InMem;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Timeouts;
// ReSharper disable ArgumentsStyleNamedExpression

namespace Rebus.AzureStorage.Tests
{
    [TestFixture]
    public class AlternativeTimeoutManager : FixtureBase
    {
        static readonly string QueueName = TestConfig.GetName("input");
        static readonly string TimeoutManagerQueueName = TestConfig.GetName("timeouts");

        BuiltinHandlerActivator _activator;
        CloudStorageAccount _storageAccount;

        protected override void SetUp()
        {
            _storageAccount = CloudStorageAccount.Parse(AzureStorageFactoryBase.ConnectionString);

            AzureStorageFactoryBase.PurgeQueue(QueueName);
            AzureStorageFactoryBase.PurgeQueue(TimeoutManagerQueueName);

            _activator = new BuiltinHandlerActivator();

            Using(_activator);
        }

        [Test]
        public async Task CanUseAlternativeTimeoutManager()
        {
            var gotTheString = new ManualResetEvent(false);

            _activator.Handle<string>(async str =>
            {
                Console.WriteLine($"Received string: '{str}'");

                gotTheString.Set();
            });

            var bus = Configure.With(_activator)
                .Transport(t =>
                {
                    var options = new AzureStorageQueuesTransportOptions { UseNativeDeferredMessages = false };

                    t.UseAzureStorageQueues(_storageAccount, QueueName, options: options);
                })
                .Timeouts(t => t.Register(c => new InMemoryTimeoutManager()))
                .Start();

            await bus.DeferLocal(TimeSpan.FromSeconds(5), "hej med dig min ven!!!!!");

            gotTheString.WaitOrDie(TimeSpan.FromSeconds(10), "Did not receive the string withing 10 s timeout");
        }

        [Test]
        public async Task CanUseDedicatedAlternativeTimeoutManager()
        {
            // start the timeout manager
            Configure.With(Using(new BuiltinHandlerActivator()))
                .Transport(t =>
                {
                    var options = new AzureStorageQueuesTransportOptions { UseNativeDeferredMessages = false };

                    t.UseAzureStorageQueues(_storageAccount, TimeoutManagerQueueName, options: options);
                })
                .Timeouts(t => t.Register(c => new InMemoryTimeoutManager()))
                .Start();

            var gotTheString = new ManualResetEvent(false);

            _activator.Handle<string>(async str =>
            {
                Console.WriteLine($"Received string: '{str}'");

                gotTheString.Set();
            });

            var bus = Configure.With(_activator)
                .Transport(t =>
                {
                    var options = new AzureStorageQueuesTransportOptions { UseNativeDeferredMessages = false };

                    t.UseAzureStorageQueues(_storageAccount, QueueName, options: options);
                })
                .Timeouts(t => t.UseExternalTimeoutManager(TimeoutManagerQueueName))
                .Start();

            await bus.DeferLocal(TimeSpan.FromSeconds(5), "hej med dig min ven!!!!!");

            gotTheString.WaitOrDie(TimeSpan.FromSeconds(10), "Did not receive the string withing 10 s timeout");

            //// don't dispose too quickly, or else we'll get annoying errors in the log
            //await Task.Delay(TimeSpan.FromSeconds(0.5));
        }
    }
}