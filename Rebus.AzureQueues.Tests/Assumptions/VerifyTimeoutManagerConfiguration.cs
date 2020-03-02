using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Persistence.InMem;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
#pragma warning disable 1998

namespace Rebus.AzureQueues.Tests.Assumptions
{
    [TestFixture]
    public class VerifyTimeoutManagerConfiguration : FixtureBase
    {
        [Test]
        public async Task CanDoIt()
        {
            var activator = Using(new BuiltinHandlerActivator());
            var gotTheMessage = Using(new ManualResetEvent(initialState: false));

            activator.Handle<string>(async str => gotTheMessage.Set());

            Configure.With(activator)
                .Transport(t =>
                {
                    var options = new AzureStorageQueuesTransportOptions { UseNativeDeferredMessages = false };
                    t.UseAzureStorageQueues(AzureConfig.StorageAccount, TestConfig.GetName("myqueue"), options);
                })
                .Timeouts(t => t.StoreInMemory())
                .Start();

            await activator.Bus.DeferLocal(TimeSpan.FromSeconds(2), "HEJ 🍗");

            gotTheMessage.WaitOrDie(timeout: TimeSpan.FromSeconds(5));
        }
    }
}