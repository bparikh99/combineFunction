using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs.Models;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Identity;
using Azure.Storage.Sas;
using Azure;
using Azure.Storage.Blobs.Specialized;


namespace Company.Function
{
    public class TimerTrigger1
    {
        [FunctionName("TimerTrigger1")]
        public static async Task  Run([TimerTrigger("0 */1 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            DateTime currentTime = DateTime.Now;
            string Container_List = Environment.GetEnvironmentVariable("ContainerList").Replace(",","\",\"");
            string storageAccountName = Environment.GetEnvironmentVariable("storageAccountName");
            string emptyContainer = Environment.GetEnvironmentVariable("emptyContainer");
            string destinationstorageAccountName = Environment.GetEnvironmentVariable("destinationstorageAccountName");
            string destinationContainer = Environment.GetEnvironmentVariable("destinationContainer");

            BlobServiceClient blobServiceClient =  null;
            BlobServiceClient outblobServiceClient =  null;
            try
            {
                blobServiceClient = new BlobServiceClient(new Uri($"https://{storageAccountName}.blob.core.windows.net"), new DefaultAzureCredential());
                
            }
            catch(Exception ex)
            {
                log.LogError($"Error While Connection to Storage Account {storageAccountName}  {ex.Message}");
            }
            
            try
            {
                outblobServiceClient = new BlobServiceClient(new Uri($"https://{destinationstorageAccountName}.blob.core.windows.net"), new DefaultAzureCredential());
                
            }
            catch(Exception ex)
            {
                log.LogError($"Error While Connection to Storage Account {destinationstorageAccountName}  {ex.Message}");
            }

            var containerList = new List<string>() {Container_List};
            List<(string, string, DateTimeOffset,long)> blobList = new List<(string, string, DateTimeOffset,long)>();

            await foreach (BlobContainerItem container in blobServiceClient.GetBlobContainersAsync())
                {
                    // log.LogInformation($"Container: {container.Name}");
                    
                    string firstMatch = containerList.FirstOrDefault(x => !x.Contains(container.Name));

                    if (firstMatch != null)
                    {
                        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(container.Name);
                        await foreach (BlobItem blob in blobServiceClient.GetBlobContainerClient(container.Name).GetBlobsAsync())
                        {
                            BlobProperties properties = await containerClient.GetBlobClient(blob.Name).GetPropertiesAsync();
                            blobList.Add((container.Name, blob.Name, properties.LastModified,properties.ContentLength));
                            log.LogInformation($"Blob: {blob.Name}, Last Modified: {properties.LastModified}, COntentLength: {properties.ContentLength}");
                                        
                        }
                    }
                                        
                }
            
            foreach (var blobInfo in blobList)
                {

                    if ( blobInfo.Item3 < currentTime )
                    {
                        log.LogInformation($"Processing file {blobInfo.Item2}");
                        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobInfo.Item1);
                        BlobClient blobClient = containerClient.GetBlobClient(blobInfo.Item2);

                        BlobContainerClient emptycontainer = blobServiceClient.GetBlobContainerClient(emptyContainer);
                        BlobClient emptyBlobClient = emptycontainer.GetBlobClient(blobInfo.Item2);

                        if(blobInfo.Item4 == 0)
                        {
                            string message = "The input stream is empty, make sure the file contains data";
                            log.LogError(message);
                            
                            bool containerExists = await emptycontainer.ExistsAsync();

                            if (!containerExists)
                            {
                                await emptycontainer.CreateAsync();
                                log.LogInformation("Container created successfully.");
                            }
                            else
                            {
                                log.LogInformation("Container already exists.");
                            }
                            var copyOp = await emptyBlobClient.StartCopyFromUriAsync(blobClient.Uri);
                            await copyOp.WaitForCompletionAsync();
                            log.LogInformation("Copied the file to empty container");

                            await blobClient.DeleteIfExistsAsync();
                            log.LogInformation("Deleted the file");
                        }
                        else{
                            BlobContainerClient destcontainer = outblobServiceClient.GetBlobContainerClient(destinationContainer);
                            BlobClient destBlobClient = destcontainer.GetBlobClient(blobInfo.Item2);
                            
                            bool containerExists = await destcontainer.ExistsAsync();

                            if (!containerExists)
                            {
                                await destcontainer.CreateAsync();
                                log.LogInformation("Container created successfully.");
                            }
                            else
                            {
                                log.LogInformation("Container already exists.");
                            }

                            await CopyAcrossStorageAccountsAsync(blobClient,destBlobClient,log);
                            log.LogInformation("Copied the file to destination/staging container");
                            await blobClient.DeleteIfExistsAsync();
                            log.LogInformation("Deleted the file");
                        }
                    }
        }
    }
            public static async Task CopyAcrossStorageAccountsAsync(
                BlobClient sourceBlob,
                BlobClient destinationBlob,ILogger log)
            {
                BlobLeaseClient sourceBlobLease = new(sourceBlob);
                Uri sourceBlobSASURI = await GenerateUserDelegationSAS(sourceBlob,log);

                try
                {
                    await sourceBlobLease.AcquireAsync(BlobLeaseClient.InfiniteLeaseDuration);

                    if(sourceBlobSASURI == null)
                    {
                        log.LogError($"Please check SAS token not generated correctly {sourceBlob.Uri}");
                    }

                    CopyFromUriOperation copyOperation = await destinationBlob.StartCopyFromUriAsync(sourceBlobSASURI);
                    await copyOperation.WaitForCompletionAsync();
                }
                catch (RequestFailedException ex)
                {
                    // Handle the 
                    log.LogError($"Error While doint copy operation for Non empty files {sourceBlob.Uri}");
                    
                }
                finally
                {
                    
                    await sourceBlobLease.ReleaseAsync();
                }
            }

            async static Task<Uri> GenerateUserDelegationSAS(BlobClient sourceBlob,ILogger log)
            {
                try
                {
                    BlobServiceClient blobServiceClient =
                    sourceBlob.GetParentBlobContainerClient().GetParentBlobServiceClient();

                    UserDelegationKey userDelegationKey =
                        await blobServiceClient.GetUserDelegationKeyAsync(DateTimeOffset.UtcNow,
                                                                        DateTimeOffset.UtcNow.AddDays(1));

                    BlobSasBuilder sasBuilder = new BlobSasBuilder()
                    {
                        BlobContainerName = sourceBlob.BlobContainerName,
                        BlobName = sourceBlob.Name,
                        Resource = "b",
                        StartsOn = DateTimeOffset.UtcNow,
                        ExpiresOn = DateTimeOffset.UtcNow.AddHours(1)
                    };

                    sasBuilder.SetPermissions(BlobSasPermissions.Read);
                    BlobUriBuilder blobUriBuilder = new BlobUriBuilder(sourceBlob.Uri)
                    {
                        
                        Sas = sasBuilder.ToSasQueryParameters(userDelegationKey,
                                                            blobServiceClient.AccountName)
                    };
                    return blobUriBuilder.ToUri();

                    
                }
                catch(Exception ex)
                {
                    log.LogError($"Error while Creating SAS token for source blob ${ex.Message} for {sourceBlob.Uri}");
                    return null;
                    
                }
                
            }
        
    }
}
