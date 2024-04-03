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
using Azure.Communication.Email;
using Azure.Messaging;


namespace Company.Function
{
    public class TimerTrigger1
    {
        [FunctionName("TimerTrigger1")]
        public static async Task  Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            DateTime currentTime = DateTime.UtcNow;
            string Container_List = Environment.GetEnvironmentVariable("ContainerList").Replace(",","\",\"");
            string storageAccountName = Environment.GetEnvironmentVariable("storageAccountName");
            string stagingstorageAccountName = Environment.GetEnvironmentVariable("stagingstorageAccountName");
            string stagingContainer = Environment.GetEnvironmentVariable("stagingContainer");
            string EmailConnection = Environment.GetEnvironmentVariable("EmailConnection");

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
                outblobServiceClient = new BlobServiceClient(new Uri($"https://{stagingstorageAccountName}.blob.core.windows.net"), new DefaultAzureCredential());
                
            }
            catch(Exception ex)
            {
                log.LogError($"Error While Connection to Storage Account {stagingstorageAccountName}  {ex.Message}");
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
                            log.LogInformation($"Blob: {blob.Name}, Last Modified: {properties.LastModified}, ContentLength: {properties.ContentLength}");
                                        
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

                        BlobContainerClient stagingcontainer = outblobServiceClient.GetBlobContainerClient(stagingContainer);
                        BlobClient stageBlobClient = stagingcontainer.GetBlobClient(blobInfo.Item2);

                        bool containerExists = await stagingcontainer.ExistsAsync();

                        if (!containerExists)
                        {
                            await stagingcontainer.CreateAsync();
                            log.LogInformation("Container created successfully.");
                        }
                        else
                        {
                            log.LogInformation("Container already exists.");
                        }

                        if(blobInfo.Item4 == 0)
                        {
                            string message = "The input stream is empty, make sure the file contains data";
                            log.LogError(message);

                            
                        }
                        await CopyAcrossStorageAccountsAsync(blobClient,stageBlobClient,log);
                        await UploadCompleted(blobClient, stageBlobClient, log);
            
                        sendEmailtoUser(EmailConnection,blobInfo.Item2,stagingContainer);
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
                    // await copyOperation.WaitForCompletionAsync();
                }
                catch (RequestFailedException ex)
                {
                    // Handle the 
                    log.LogError($"Error While doing copy operation for files {sourceBlob.Uri} {ex.Message}");
                    
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
                                                                        DateTimeOffset.UtcNow.AddHours(1));

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
            private static void sendEmailtoUser(string EmailConnection , string blobName, string containerName )
            {
                string fromEmail = Environment.GetEnvironmentVariable("fromEmail");
                string toEmail = Environment.GetEnvironmentVariable("toEmail");
                
                var emailClient = new EmailClient(EmailConnection);
                string plainTextContent = "";

                plainTextContent = $"{blobName} is moved to container Name {containerName}. \n\nThis is an automatically generated email – please do not reply to it. If you have any queries please email {fromEmail}";

                EmailSendOperation emailSendOperation = emailClient.Send(
                    WaitUntil.Completed,
                    senderAddress: fromEmail,
                    recipientAddress: toEmail,
                    subject: "File Moved From Landing to Staging Area",
                    htmlContent: "",
                    plainTextContent: plainTextContent);

            }

            private static async Task UploadCompleted(BlobClient blobClient , BlobClient destBlobClient, ILogger log) 
            {
                bool isBlobCopiedSuccessfully = false;

                do
                {
                    log.LogInformation("Checking copy status....");
                    var targetBlobProperties = await destBlobClient.GetPropertiesAsync();
                    log.LogInformation($"Current copy status = {targetBlobProperties.Value.CopyStatus}");
                    if (targetBlobProperties.Value.CopyStatus.Equals(CopyStatus.Pending))
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                    else
                    {
                        isBlobCopiedSuccessfully = targetBlobProperties.Value.CopyStatus.Equals(CopyStatus.Success);
                        break;
                }
                } while (true);

                if (isBlobCopiedSuccessfully)
                {
                    log.LogInformation("Blob copied successfully. Now deleting source blob...");
                    await blobClient.DeleteAsync();
                }
            }
             
    }
}
