# Rebus.AzureQueues

[![install from nuget](https://img.shields.io/nuget/v/Rebus.AzureQueues.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.AzureQueues)


Provides an Azure Storage Queues-based transport implementation for [Rebus](https://github.com/rebus-org/Rebus).

It's just

```csharp
var storageAccount = CloudStorageAccount.Parse(connectionString);

services.AddRebus(
	configure => configure
		.Transport(t => t.UseAzureStorageQueues(storageAccount, "your_queue"))
		.(...)
);

```

or

```csharp
var storageAccount = CloudStorageAccount.Parse(connectionString);

services.AddRebus(
	configure => configure
		.Transport(t => t.UseAzureStorageQueuesAsOneWayClient(storageAccount))
		.(...)
);
```

and off you go! :rocket:

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---


