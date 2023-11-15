# Changelog

## 0.2.0
* Test release

## 0.3.0
* Fix queue purge bug - thanks [asleire]

## 0.4.0
* Make it possible to take over queue creation by providing a queue factory - thanks [asleire]

## 0.5.0
* Update Microsoft.Azure.Storage.Queue dependency to 11.1.1 (which is a breaking change...) - thanks [asleire]

## 0.6.0
* Add ability to skip queue creation - thanks [asleire]

## 1.0.0
* Update to Rebus 6 - thanks [asleire]

## 1.1.0
* Update Microsoft.Azure.Storage.Queue dependency to 11.2.2
* Remove unnecessary explicit dependencies on Microsoft.Azure.Storage.Common and Microsoft.Azure.KeyVault.Core

## 2.0.1
* Only target .NET Standard 2.0
* Add ability to automatically renew peek locks - thanks [hdrachmann]

## 3.0.0
* Switch to Azure.Storage.Queues

## 3.1.0
* Fix ability to use native defer without relying on own input queue (and thus fix the error that made one-way clients unable to use native defer)

## 4.0.0
* Update to Rebus 8
* Add configuration overloads to enable passing a `TokenCredential` and a `Uri` instead of a connection string, thus enabling the use of managed identities etc. - thanks [mirandaasm]
* Update Azure.Storage.Queues to 12.17.1

[asleire]: https://github.com/asleire
[hdrachmann]: https://github.com/hdrachmann
[mirandaasm]: https://github.com/mirandaasm
