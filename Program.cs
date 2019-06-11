using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Google.Api.Gax;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using Object = System.Object;

namespace landsat
{
    class Program
    {
        static void Main(string[] args)
        {
            // If/else dir cache:                114188.51ms, 189050.292ms
            // If dir cache:                      38039.178ms
            // No dir cache:                      35979.628ms, 37687.228ms
            // No dir cache, no output:           34946.14ms
            var startTime = DateTime.Now;
            const int objectLimit = 2000;
            const string downloadDir = "./data";
            const string bucketName = "gcp-public-data-landsat";

            DownloadFromGcs(downloadDir, bucketName, ".txt", objectLimit);

            var duration = DateTime.Now - startTime;
            Console.WriteLine($"Took {duration.TotalMilliseconds}ms to download {objectLimit} objects");
            Environment.Exit(0);
        }

        private static void DownloadFromGcs(string downloadDir, string bucketName, string filter = ".txt",
            int limit = 1000)
        {
            var storageClient = StorageClient.Create();
            var objects = storageClient.ListObjects(bucketName);

            if (objects == null)
            {
                return;
            }

            downloadDir = Path.GetFileName(downloadDir);

            var objectCount = 0;

            foreach (var o in objects)
            {
                if (objectCount == limit)
                {
                    break;
                }

                if (!o.Name.EndsWith(filter))
                {
                    continue;
                }

                var destinationFile = Path.Combine(downloadDir, o.Name);
                var destinationDir = string.Join("/", destinationFile.Split("/").SkipLast(1));

                // Blindly call even if directory already exists
                Directory.CreateDirectory(destinationDir);

                using (var outputFile = File.OpenWrite(destinationFile))
                {
                    storageClient.DownloadObject(bucketName, o.Name, outputFile);
                }

                objectCount++;
            }
        }
    }
}