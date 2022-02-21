using System;

namespace Rebus.AzureQueues.Tests.Extensions;

static class TestEx
{
    public static IDisposable AsDisposable<T>(this T disposable, Action<T> disposeAction) => new DisposableCallback<T>(disposable, disposeAction);

    struct DisposableCallback<T> : IDisposable
    {
        readonly T _instance;
        readonly Action<T> _action;

        public DisposableCallback(T instance, Action<T> action)
        {
            _instance = instance;
            _action = action;
        }

        public void Dispose() => _action(_instance);
    }
}