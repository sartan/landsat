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
            objects.AsParallel().ForAll(o => WriteObjectToFileSystem(bucket, o, destination));
        }

        private void WriteObjectToFileSystem(string bucket, GcsObject obj, string destinationRootDir)
        {
            var destinationFile = Path.Combine(destinationRootDir, obj.Name);
            var destinationDir = string.Join("/", destinationFile.Split("/").SkipLast(1));

            // Blindly call even if directory already exists
            Directory.CreateDirectory(destinationDir);

            using (var outputFile = File.OpenWrite(destinationFile))
            {
                _storageClient.DownloadObject(bucket, obj.Name, outputFile);
                Console.WriteLine($"Writing file: {destinationFile}");
            }
        }
    }
}