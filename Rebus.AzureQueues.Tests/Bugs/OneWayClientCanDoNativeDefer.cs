using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.AzureQueues.Tests.Bugs;

[TestFixture]
public class OneWayClientCanDoNativeDefer : FixtureBase
{
    [Test]
    public async Task CanDoIt()
    {
        using var activator = new BuiltinHandlerActivator();
        using var gotTheString = new ManualResetEvent(initialState: false);

        activator.Handle<string>(async _ => gotTheString.Set());

        var queueName = "receiver";

        Using(new QueueDeleter(queueName));

        Configure.With(activator)
            .Transport(t => t.UseAzureStorageQueues(AzureConfig.ConnectionString, queueName))
            .Start();

        var client = Configure.With(Using(new BuiltinHandlerActivator()))
            .Transport(t => t.UseAzureStorageQueuesAsOneWayClient(AzureConfig.ConnectionString))
            .Routing(r => r.TypeBased().Map<string>(queueName))
            .Start();

        await client.Defer(TimeSpan.FromSeconds(2), "hej med dig, min ven!");

        gotTheString.WaitOrDie(TimeSpan.FromSeconds(5));
    }

}