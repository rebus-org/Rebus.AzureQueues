using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

#pragma warning disable 1998

namespace Rebus.AzureQueues.Tests.Transport;

[TestFixture]
public class NativeDeferTest : FixtureBase
{
    static readonly string QueueName = TestConfig.GetName("input");

    BuiltinHandlerActivator _activator;
    IBusStarter _busStarter;

    protected override void SetUp()
    {
        _activator = new BuiltinHandlerActivator();

        Using(_activator);

        _busStarter = Configure.With(_activator)
            .Transport(t => t.UseAzureStorageQueues(AzureConfig.ConnectionString, QueueName))
            .Routing(r => r.TypeBased().Map<TimedMessage>(QueueName))
            .Options(o =>
            {
                o.LogPipeline();
            })
            .Create();
    }

    [Test]
    public async Task UsesNativeDeferralMechanism()
    {
        var done = new ManualResetEvent(false);
        var receiveTime = DateTimeOffset.MinValue;
        var hadDeferredUntilHeader = false;

        _activator.Handle<TimedMessage>(async (bus, context, message) =>
        {
            receiveTime = DateTimeOffset.Now;

            hadDeferredUntilHeader = context.TransportMessage.Headers.ContainsKey(Headers.DeferredUntil);

            done.Set();
        });

        var bus = _busStarter.Start();

        var sendTime = DateTimeOffset.Now;

        await bus.Defer(TimeSpan.FromSeconds(5), new TimedMessage { Time = sendTime });

        done.WaitOrDie(TimeSpan.FromSeconds(8), "Did not receive 5s-deferred message within 8 seconds of waiting....");

        var delay = receiveTime - sendTime;

        Console.WriteLine($"Message was delayed {delay}");

        Assert.That(delay, Is.GreaterThan(TimeSpan.FromSeconds(4)), "The message not delayed ~5 seconds as expected!");
        Assert.That(delay, Is.LessThan(TimeSpan.FromSeconds(8)), "The message not delayed ~5 seconds as expected!");

        Assert.That(hadDeferredUntilHeader, Is.False, $"Received message still had the '{Headers.DeferredUntil}' header - we must remove that");
    }

    class TimedMessage
    {
        public DateTimeOffset Time { get; set; }
    }
}