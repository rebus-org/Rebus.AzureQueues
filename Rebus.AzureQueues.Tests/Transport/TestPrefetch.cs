using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;

// ReSharper disable ArgumentsStyleNamedExpression
#pragma warning disable 1998

namespace Rebus.AzureQueues.Tests.Transport
{
    [TestFixture]
    public class TestPrefetch : FixtureBase
    {
        const string QueueName = "test-prefetch";
        static readonly object DummyObject = new object();

        protected override void SetUp()
        {
            AzureStorageFactoryBase.PurgeQueue(QueueName);
        }

        [TestCase(true, 200, 4, 30)]
        [TestCase(false, 200, 4, 30)]
        public async Task RunTest(bool prefetch, int messageCount, int workers, int parallelism)
        {
            var counter = new SharedCounter(messageCount);
            var activator = new BuiltinHandlerActivator();
            var dictionary = new ConcurrentDictionary<string, object>();

            Using(activator);

            activator.Handle<string>(async str =>
            {
                dictionary.TryRemove(str, out _);
                counter.Decrement();
            });

            var bus = Configure.With(activator)
                .Logging(l => l.Console(minLevel: LogLevel.Warn))
                .Transport(t =>
                {
                    var options = new AzureStorageQueuesTransportOptions
                    {
                        Prefetch = prefetch ? 32 : default(int?)
                    };

                    t.UseAzureStorageQueues(AzureStorageFactoryBase.ConnectionString, QueueName, options);
                })
                .Options(o =>
                {
                    o.SetNumberOfWorkers(0);
                    o.SetMaxParallelism(parallelism);
                })
                .Start();

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(async i =>
                {
                    var payload = $"THIS IS MESSAGE {i}";
                    dictionary[payload] = DummyObject;
                    await bus.SendLocal(payload);
                }));

            var stopwatch = Stopwatch.StartNew();

            bus.Advanced.Workers.SetNumberOfWorkers(workers);

            counter.WaitForResetEvent(120);

            var elapsedSeconds = stopwatch.Elapsed.TotalSeconds;

            Console.WriteLine($@"Receiving {messageCount} took {elapsedSeconds:0.0} s - that's {messageCount / elapsedSeconds:0.0} msg/s");
        }
    }
}