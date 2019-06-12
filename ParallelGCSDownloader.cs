using System;
using System.IO;
using System.Linq;
using Google.Cloud.Storage.V1;
using GcsObject = Google.Apis.Storage.v1.Data.Object;

namespace landsat
{
    public class ParallelGcsDownloader : IGcsDownloader
    {
        private readonly StorageClient _storageClient;
        private static readonly int MaxRetries = 3;

        public ParallelGcsDownloader(StorageClient storageClient)
        {
            _storageClient = storageClient;
        }

        public void Download(string bucket, string prefix, string destination)
        {
            var objects = _storageClient.ListObjects(bucket, prefix);

            if (objects == null)
            {
                return;
            }

            destination = Path.GetFileName(destination);
            objects.Where(o => o.Name.EndsWith(".txt")).Take(2000).AsParallel()
                .ForAll(o => DownloadObject(bucket, o, destination));
        }

        private void DownloadObject(string bucket, GcsObject obj, string destinationRootDir)
        {
            var destinationFile = Path.Combine(destinationRootDir, obj.Name);
            var destinationDir = string.Join("/", destinationFile.Split("/").SkipLast(1));

            // Blindly call even if directory already exists
            Directory.CreateDirectory(destinationDir);

            var retriesLeft = MaxRetries;

            while (true)
            {
                try
                {
                    TryDownloadObject(bucket, obj, destinationFile);
                    break;
                }
                // TODO: Handle specific exceptions
                catch
                {
                    if (--retriesLeft == 0)
                    {
                        // Intentionally swallow exception, so that other downloads can proceed
                        // TODO: Log original exception
                        Console.WriteLine($"Failed to download {destinationFile}");
                    }

                    // TODO: Sleep before retrying?
                }
            }
        }

        private void TryDownloadObject(string bucket, GcsObject obj, string destinationFile)
        {
            using (var outputFile = File.OpenWrite(destinationFile))
            {
                _storageClient.DownloadObject(bucket, obj.Name, outputFile);
                Console.WriteLine($"Writing file: {destinationFile}");
            }
        }
    }
}