using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Persistence.InMem;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Time;
using Rebus.Timeouts;
#pragma warning disable CS1998

// ReSharper disable ArgumentsStyleNamedExpression

namespace Rebus.AzureQueues.Tests;

[TestFixture]
public class AlternativeTimeoutManager : FixtureBase
{
    static readonly string QueueName = TestConfig.GetName("input");
    static readonly string TimeoutManagerQueueName = TestConfig.GetName("timeouts");

    BuiltinHandlerActivator _activator;

    protected override void SetUp()
    {
        AzureConfig.PurgeQueue(QueueName);
        AzureConfig.PurgeQueue(TimeoutManagerQueueName);

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

                t.UseAzureStorageQueues(AzureConfig.ConnectionString, QueueName, options: options);
            })
            .Timeouts(t => t.Register(c => new InMemoryTimeoutManager(new DefaultRebusTime())))
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

                t.UseAzureStorageQueues(AzureConfig.ConnectionString, TimeoutManagerQueueName, options: options);
            })
            .Timeouts(t => t.Register(c => new InMemoryTimeoutManager(new DefaultRebusTime())))
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

                t.UseAzureStorageQueues(AzureConfig.ConnectionString, QueueName, options: options);
            })
            .Timeouts(t => t.UseExternalTimeoutManager(TimeoutManagerQueueName))
            .Start();

        await bus.DeferLocal(TimeSpan.FromSeconds(5), "hej med dig min ven!!!!!");

        gotTheString.WaitOrDie(TimeSpan.FromSeconds(10), "Did not receive the string withing 10 s timeout");

        //// don't dispose too quickly, or else we'll get annoying errors in the log
        //await Task.Delay(TimeSpan.FromSeconds(0.5));
    }
}