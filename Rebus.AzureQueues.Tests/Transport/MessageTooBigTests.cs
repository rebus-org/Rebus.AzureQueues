using NUnit.Framework;
using Rebus.Activation;
using Rebus.AzureQueues.Transport;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Threading.SystemThreadingTimer;
using Rebus.Extensions;
using Rebus.Time;
using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using System.Linq;
using Rebus.Retry.Simple;
using Rebus.Tests.Contracts.Extensions;
// ReSharper disable ArgumentsStyleLiteral

// ReSharper disable AccessToDisposedClosure

namespace Rebus.AzureQueues.Tests.Transport
{
    [TestFixture]
    public class MessageTooBigTests : FixtureBase
    {
        static readonly string ConnectionString = AzureConfig.ConnectionString;

        readonly TimeSpan InitialTimeout = TimeSpan.FromSeconds(6);
        readonly string queueName = TestConfig.GetName("input");
        readonly string errorQueueName = TestConfig.GetName("myerrorqueue");

        AzureStorageQueuesTransport _errorQueueListener;
        BuiltinHandlerActivator _activator;
        ListLoggerFactory _listLoggerFactory;
        IBusStarter _busStarter;
        IBus _bus;

        protected override void SetUp()
        {
            _listLoggerFactory = new ListLoggerFactory(outputToConsole: true, detailed: true);

            _errorQueueListener = new AzureStorageQueuesTransport(new ConnectionStringQueueClientFactory(AzureConfig.ConnectionString), errorQueueName, _listLoggerFactory, new AzureStorageQueuesTransportOptions(), new DefaultRebusTime(), new SystemThreadingTimerAsyncTaskFactory(_listLoggerFactory));

            Using(_errorQueueListener);

            _errorQueueListener.Initialize();
            _errorQueueListener.PurgeInputQueue();

            _activator = Using(new BuiltinHandlerActivator());

            _busStarter = Configure.With(_activator)
                .Logging(l => l.Use(_listLoggerFactory))
                .Transport(t => t.UseAzureStorageQueues(ConnectionString, queueName, new AzureStorageQueuesTransportOptions
                {
                    AutomaticPeekLockRenewalEnabled = true,
                    InitialVisibilityDelay = InitialTimeout,
                }))
                .Options(o =>
                {
                    o.SimpleRetryStrategy(errorQueueName);
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);
                })
                .Create();

            _bus = _busStarter.Bus;
        }

        [Test]
        public async Task Should_put_incoming_message_on_error_queue_when_outgoing_is_too_big_WithoutRenewal()
        {
            using var gotCalledFiveTimes = new CountdownEvent(5);

            _activator.Handle<string>(async (innerBus, context, _) =>
            {
                Console.WriteLine($"Got message with ID {context.Headers.GetValue(Headers.MessageId)} - Sending off huge message");

                var hugeMessage = GetHugeMessage();
                await innerBus.SendLocal(hugeMessage);
                gotCalledFiveTimes.Signal();

                Console.WriteLine($"{gotCalledFiveTimes.CurrentCount} tries left");
            });

            _busStarter.Start();

            await _bus.SendLocal("Get going....");

            gotCalledFiveTimes.Wait(TimeSpan.FromSeconds(120));

            //There should now be a message in the error queue.
            _ = await _errorQueueListener.WaitForNextMessage(timeoutSeconds: 5);

            //the error should be logged
            Assert.IsTrue(_listLoggerFactory.Any(l => l.Level == LogLevel.Error));
        }

        [Test]
        public async Task Should_put_incoming_message_on_error_queue_when_outgoing_is_too_big_WithRenewal()
        {
            using var gotCalledFiveTimes = new CountdownEvent(5);

            _activator.Handle<string>(async (innerBus, context, _) =>
            {
                Console.WriteLine($"Got message with ID {context.Headers.GetValue(Headers.MessageId)} - Sending off huge message");

                //first wait to ensure renewal has been effectuated
                await Task.Delay(InitialTimeout + TimeSpan.FromSeconds(10));

                var hugeMessage = GetHugeMessage();
                await innerBus.SendLocal(hugeMessage);
                gotCalledFiveTimes.Signal();

                Console.WriteLine($"{gotCalledFiveTimes.CurrentCount} tries left");
            });

            _busStarter.Start();

            await _bus.SendLocal("Get going....");

            gotCalledFiveTimes.Wait(TimeSpan.FromSeconds(120));

            //There should now be a message in the error queue.
            _ = await _errorQueueListener.WaitForNextMessage(timeoutSeconds: 5);

            //the error should be logged
            Assert.IsTrue(_listLoggerFactory.Any(l => l.Level == LogLevel.Error));
        }

        static HugeMessage GetHugeMessage()
        {
            var bytes = new byte[64_000];
            Random.Shared.NextBytes(bytes);
            var hugeMessage = new HugeMessage(bytes);
            return hugeMessage;
        }

        public class HugeMessage
        {
            public HugeMessage(byte[] bytes)
            {
                Bytes = bytes;
            }
            public byte[] Bytes { get; set; }
        }
    }
}
