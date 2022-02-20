using System;
using Rebus.Bus;

namespace Rebus.Config
{
    /// <summary>
    /// Options to configure behavior of Rebus' Azure Storage Queues transport
    /// </summary>
    public class AzureStorageQueuesTransportOptions
    {
        /// <summary>
        /// Configures whether Azure Storage Queues' built-in support for deferred messages should be used.
        /// Defaults to <code>true</code>. When set to <code>false</code>, please remember to register
        /// a timeout manager, or configure another endpoint as a timeout manager, if you intend to
        /// <see cref="IBus.Defer"/> or <see cref="IBus.DeferLocal"/> messages.
        /// </summary>
        public bool UseNativeDeferredMessages { get; set; } = true;

        /// <summary>
        /// Configures how many messages to prefetch. Valid values are null, 0, ... 32
        /// </summary>
        public int? Prefetch { get; set; } = null;

        /// <summary>
        /// Optional. Specifies the new visibility timeout value, in seconds, relative to server time. The default value is 5 minutes.
        /// A specified value must be larger than or equal to 1 second, and cannot be larger than 7 days, or larger than 2 hours on REST protocol versions prior to version 2011-08-18. The
        /// visibility timeout of a message can be set to a value later than the expiry time.
        /// </summary>
        /// <remarks>
        /// See https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages for more
        /// </remarks>
        public TimeSpan InitialVisibilityDelay { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Configures whether the transport will skip queue checking and creation.
        /// Defaults to <code>true</code>, which means that the input queue and the error queue will be created automatically.
        /// Should be set to <code>false</code> if your application is not authorized to create queues on the Azure Storage Account. 
        /// </summary>
        public bool AutomaticallyCreateQueues { get; set; } = true;

        /// <summary>
        /// Enables automatic peek lock renewal. Only enable this if you intend on handling messages for a long long time, and
        /// DON'T intend on handling messages quickly - it will have an impact on message receive, so only enable it if you
        /// need it. You should usually strive after keeping message processing times low, much lower than the 5 minute lease
        /// you get with Azure Queue. Will not work with prefecth of messages.
        /// </summary>
        public bool AutomaticPeekLockRenewalEnabled { get; set; } = false;

    }
}