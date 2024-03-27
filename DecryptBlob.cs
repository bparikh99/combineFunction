using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using PgpCore;
using System.Text;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs.Models;

namespace Company.Function
{
    public class DecryptBlob
    {
        [FunctionName("DecryptBlob")]
        public static async Task Run([BlobTrigger("staging/{name}", Connection = "BlobTriggerString")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            
            string keyVaultName = Environment.GetEnvironmentVariable("keyVaultName");
            string stagingstorageAccountName = Environment.GetEnvironmentVariable("stagingstorageAccountName");
            string privateKey = Environment.GetEnvironmentVariable("privateKeyName");
            string privateKeyPassPhrase =Environment.GetEnvironmentVariable("passPhraseKeyName");
            string stagingContainer =  Environment.GetEnvironmentVariable("stagingContainer");
            string decryptContainer =  Environment.GetEnvironmentVariable("decryptContainer");
            

            try {
                var kvUri = "https://" + keyVaultName + ".vault.azure.net";
                var client = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
                
                var blobServiceClient = new BlobServiceClient(new Uri($"https://{stagingstorageAccountName}.blob.core.windows.net"), new DefaultAzureCredential());

                var DestblobServiceClient = new BlobServiceClient(new Uri($"https://{stagingstorageAccountName}.blob.core.windows.net"), new DefaultAzureCredential());

                var privateKeysecretValue = await client.GetSecretAsync(privateKey);
                string keyContent = privateKeysecretValue.Value.Value;

                var passphraseSecretValue = await client.GetSecretAsync(privateKeyPassPhrase);
                string pphrase = passphraseSecretValue.Value.Value;

                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(stagingContainer);
                BlobContainerClient outcontainerClient = DestblobServiceClient.GetBlobContainerClient(decryptContainer);

                bool containerExists = await outcontainerClient.ExistsAsync();

                if (!containerExists)
                {
                    await outcontainerClient.CreateAsync();
                    log.LogInformation("Container created successfully.");
                }
                else
                {
                    log.LogInformation("Container already exists.");
                }
                
                BlobClient InblobClient = containerClient.GetBlobClient(name);
                BlobClient outblobClient = outcontainerClient.GetBlobClient(name);
                
                var propertiesResponse = InblobClient.GetProperties();
                var inputString = "";
              
                using (StreamReader reader = new StreamReader(myBlob))
                {
                    inputString = reader.ReadToEnd();
                }


                if (IsEncrypted(inputString))
                    {
                        log.LogInformation("Validation for The file is encrypted is passed!");

                        Stream inputStream = GenerateStreamFromString(inputString);

            
                        byte[] privateKeyBytes = Convert.FromBase64String(keyContent);
                        string privateKeyEncoded = Encoding.UTF8.GetString(privateKeyBytes);

                        Stream decryptedData = await DecryptAsync(inputStream, privateKeyEncoded, pphrase, log);
                        outblobClient.Upload(decryptedData, overwrite: true);  
                        log.LogInformation($"File Decrypted Successfully {name}");     

                    }
                else
                    {
                        log.LogInformation("The file is not encrypted. Copying as it is to the destination folder.");

                        BlobDownloadInfo blobDownloadInfo = await InblobClient.DownloadAsync();

                        using (MemoryStream memoryStream = new MemoryStream())
                        {
                            await blobDownloadInfo.Content.CopyToAsync(memoryStream);
                            memoryStream.Position = 0;

                            await outblobClient.UploadAsync(memoryStream, true);
                        }
                    }
                bool checkExists = await outblobClient.ExistsAsync();
                if(checkExists)
                {
                    log.LogInformation("File Uploaded completed");
                    await InblobClient.DeleteAsync();
                    log.LogInformation("File Deleted Successfully from staging container");
                }
            }
            catch (Exception ex)
            {

                log.LogError(ex, "There is an error occurred");
               
            }
        }
    
        [Obsolete]
        private static async Task<Stream> DecryptAsync(Stream inputStream, string privateKey, string pphrase,ILogger log)
        {
            using (PGP pgp = new PGP())
            {
                Stream outputStream = new MemoryStream();
                log.LogInformation("inside decrypt func");
                using (inputStream)
                using (Stream privateKeyStream = GenerateStreamFromString(privateKey))
                {
                    try{
                        
                        await pgp.DecryptStreamAsync(inputStream, outputStream, privateKeyStream, pphrase);
                    }
                    catch(Exception ex)
                    {
                        log.LogError(ex,"Error while Decrypt");
                    }
                    
                    
                    outputStream.Seek(0, SeekOrigin.Begin);
                    return outputStream;
                }
            }
        }
        private static Stream GenerateStreamFromString(string s)
        {
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
        private static bool IsEncrypted(string content)
        {
            // You need to implement a logic to determine if the file is encrypted or not based on its content or other characteristics.
            // For example, you might check if the content starts with a specific prefix that indicates encryption.
            // Modify this logic based on how you determine if a file is encrypted.
            return content.StartsWith("-----BEGIN PGP MESSAGE-----");
        }
    }
}

