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
using Rebus.Transport;
using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Tests.Contracts.Extensions;
using System.Linq;
using Rebus.Retry.Simple;

namespace Rebus.AzureQueues.Tests.Transport
{
    [TestFixture]
    public class MessageTooBigTests : FixtureBase
    {

        static readonly string ConnectionString = AzureConfig.ConnectionString;
        static readonly string QueueName = TestConfig.GetName("input");



        BuiltinHandlerActivator _activator;
        AzureStorageQueuesTransport _errorQueueListener;
        ListLoggerFactory _listLoggerFactory;
        IBus _bus;
        IBusStarter _busStarter;
        private const string MyErrorQueueName = "myerrorqueue";
        private TimeSpan InitialTimeout = TimeSpan.FromSeconds(6);
        protected override void SetUp()
        {
            _listLoggerFactory = new ListLoggerFactory(outputToConsole: true, detailed: true);

            _errorQueueListener = new AzureStorageQueuesTransport(AzureConfig.StorageAccount, MyErrorQueueName, _listLoggerFactory, new AzureStorageQueuesTransportOptions(), new DefaultRebusTime(), new SystemThreadingTimerAsyncTaskFactory(new ConsoleLoggerFactory(false)));
            _errorQueueListener.Initialize();
            _errorQueueListener.PurgeInputQueue();

            _activator = Using(new BuiltinHandlerActivator());

            _busStarter = Configure.With(_activator)
                .Logging(l => l.Use(_listLoggerFactory))
                .Transport(t => t.UseAzureStorageQueues(ConnectionString, QueueName, new AzureStorageQueuesTransportOptions()
                {
                    AutomaticPeekLockRenewalEnabled = true,
                    InitialVisibilityDelay = InitialTimeout,

                }))
                .Options(o =>
                {
                    o.SimpleRetryStrategy(MyErrorQueueName);
                    o.SetNumberOfWorkers(1);
                    o.SetMaxParallelism(1);

                })
                .Create();

            _bus = _busStarter.Bus;
        }

        private Random _random = new Random();
        [Test]

        public async Task Should_put_incoming_message_on_error_queue_when_outgoing_is_too_big()
        {
            using var gotCalledFiveTimes = new CountdownEvent(5);
            _activator.Handle<string>(async (innerBus, context, _) =>
            {
                Console.WriteLine($"Got message with ID {context.Headers.GetValue(Headers.MessageId)} - Sending off huge message");

                //first wait to ensure renewal has been effectuated
                await Task.Delay(InitialTimeout + TimeSpan.FromSeconds(10));

                byte[] bytes = new byte[64_000];
                _random.NextBytes(bytes);
                await innerBus.SendLocal(new HugeMessage(bytes));
                gotCalledFiveTimes.Signal();
                Console.WriteLine(gotCalledFiveTimes.CurrentCount + " tries left");
            });

            _busStarter.Start();

            await _bus.SendLocal("Get going....");


            gotCalledFiveTimes.Wait(TimeSpan.FromSeconds(120));

            //There should now be a message in the error queue.
            using var scope = new RebusTransactionScope();
            var message = await _errorQueueListener.Receive(scope.TransactionContext, CancellationToken.None);
            await scope.CompleteAsync();

            if (message == null)
            {
                throw new AssertionException(
                    $"Did not receive a message.");
            }

            //make absolutely sure that the transaction has finished
            await Task.Delay(TimeSpan.FromSeconds(5));

            //the error should be logged
            Assert.IsTrue(_listLoggerFactory.Any(l => l.Level == LogLevel.Error));

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
