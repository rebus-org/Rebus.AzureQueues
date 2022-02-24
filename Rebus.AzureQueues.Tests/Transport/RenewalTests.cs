using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using System;
using Rebus.Tests.Contracts;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Transport;
using Rebus.AzureQueues.Transport;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Messages;
using Rebus.Extensions;
using Rebus.Threading.SystemThreadingTimer;
using Rebus.Tests.Contracts.Extensions;
using System.Linq;

namespace Rebus.AzureQueues.Tests.Transport;

[TestFixture]
public class AzureQueuePeekLockRenewalTest : FixtureBase
{
    static readonly string ConnectionString = AzureConfig.ConnectionString;
    static readonly string QueueName = TestConfig.GetName("input");

    readonly ConsoleLoggerFactory _consoleLoggerFactory = new ConsoleLoggerFactory(false);

    BuiltinHandlerActivator _activator;
    AzureStorageQueuesTransport _transport;
    ListLoggerFactory _listLoggerFactory = new ListLoggerFactory(outputToConsole: true, detailed: true);
    IBus _bus;
    IBusStarter _busStarter;
    TimeSpan _visibilityTimeout = TimeSpan.FromSeconds(20);
    protected override void SetUp()
    {
        _transport = new AzureStorageQueuesTransport(AzureConfig.StorageAccount, QueueName, _consoleLoggerFactory, new AzureStorageQueuesTransportOptions(), new Time.DefaultRebusTime(), new SystemThreadingTimerAsyncTaskFactory(new ConsoleLoggerFactory(false)));

        _transport.Initialize();
        _transport.PurgeInputQueue();

        _activator = new BuiltinHandlerActivator();

        _busStarter = Configure.With(_activator)
            .Logging(l => l.Use(_listLoggerFactory))
            .Transport(t => t.UseAzureStorageQueues(ConnectionString, QueueName, new AzureStorageQueuesTransportOptions()
            {
                AutomaticPeekLockRenewalEnabled = true,
                InitialVisibilityDelay = _visibilityTimeout
            }))
            .Options(o =>
            {
                o.SetNumberOfWorkers(1);
                o.SetMaxParallelism(1);
            })
            .Create();

        _bus = _busStarter.Bus;

        Using(_bus);
    }


    [Test]
    public async Task ItWorks()
    {
        var gotMessage = new ManualResetEvent(false);

        _activator.Handle<string>(async (bus, context, message) =>
        {
            Console.WriteLine($"Got message with ID {context.Headers.GetValue(Headers.MessageId)} - waiting timout + 30 secs minutes....");


            await Task.Delay(_visibilityTimeout + TimeSpan.FromSeconds(30));

            Console.WriteLine("done waiting");

            gotMessage.Set();
        });

        _busStarter.Start();

        await _bus.SendLocal("hej med dig min ven!");

        //would appear after visibility timout - if it wasn't  extended
        await Task.Delay(_visibilityTimeout + TimeSpan.FromSeconds(2));

        
        // see if queue is empty
        using var scope = new RebusTransactionScope();

        var message = await _transport.Receive(scope.TransactionContext, CancellationToken.None);

        scope.Complete();

        if (message != null)
        {
            throw new AssertionException(
                $"Did not expect to receive a message - got one with ID {message.Headers.GetValue(Headers.MessageId)}");
        }

        gotMessage.WaitOrDie(_visibilityTimeout + TimeSpan.FromSeconds(35));

        //make absolutely sure that the transaction has finished
        await Task.Delay(TimeSpan.FromSeconds(10));
       
        Assert.IsFalse(_listLoggerFactory.Any(l => l.Level == LogLevel.Error),"had an error when handling the message.. check the logs");



    }
}