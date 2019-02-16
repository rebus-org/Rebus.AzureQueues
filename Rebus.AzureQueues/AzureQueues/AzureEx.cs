using System.Net;
using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Table;

namespace Rebus.AzureQueues
{
    static class AzureEx
    {
        //public static void EnsureOk(this TableResult result)
        //{
        //    if (result.HttpStatusCode == 200) return;

        //    throw new RebusApplicationException($"Table result status: {result.HttpStatusCode} - expected 200 OK");
        //}

        public static bool IsStatus(this StorageException exception, HttpStatusCode statusCode)
        {
            //var httpRequestException = exception.InnerException as System.Net.Http.HttpRequestException;
            //return false;

            return exception.RequestInformation.HttpStatusCode == (int) statusCode;

            //var webException = exception.InnerException as WebException;
            //var response = webException?.Response as HttpWebResponse;
            //return response?.StatusCode == statusCode;
        }
    }
}