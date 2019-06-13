using System;
using System.IO.Abstractions;
using System.Linq;
using Google.Cloud.Storage.V1;
using GcsObject = Google.Apis.Storage.v1.Data.Object;

namespace GCSDownload
{
    public class ParallelDownloader : IDownloader
    {
        private readonly StorageClient _storageClient;
        private readonly IFileSystem _fileSystem;
        private static readonly int MaxRetries = 3;

        public ParallelDownloader(StorageClient storageClient, IFileSystem fileSystem)
        {
            _storageClient = storageClient;
            _fileSystem = fileSystem;
        }

        public void Download(string bucket, string prefix, string destination)
        {
            var objects = _storageClient.ListObjects(bucket, prefix);

            if (objects == null)
            {
                return;
            }

            var destinationAbsPath = _fileSystem.Path.GetFullPath(destination);
            objects.AsParallel().ForAll(o => DownloadObject(bucket, o, destinationAbsPath));
        }

        private void DownloadObject(string bucket, GcsObject obj, string destinationRootDir)
        {
            var destinationFile = _fileSystem.Path.Combine(destinationRootDir, obj.Name.TrimStart('/'));
            var destinationDir = string.Join("/", destinationFile.Split("/").SkipLast(1));

            // Blindly call even if directory already exists
            _fileSystem.Directory.CreateDirectory(destinationDir);

            var retriesLeft = MaxRetries;

            while (true)
            {
                try
                {
                    // TODO: Split downloading and writing to disk concerns
                    // This would allow us to do the two asynchronously
                    // and handle exceptions/retries more intelligently.
                    // For example, if download fails we can re-try,
                    // but if the disk is not writable, don't bother.
                    TryDownloadObject(bucket, obj, destinationFile);
                    break;
                }
                // TODO: Handle specific exceptions
                catch
                {
                    if (--retriesLeft == 0)
                    {
                        // TODO: Log original exception
                        // Intentionally swallow exception, so that other downloads can proceed
                        Console.WriteLine($"Failed to download {destinationFile}");
                    }

                    // TODO: Sleep before retrying?
                }
            }
        }

        private void TryDownloadObject(string bucket, GcsObject obj, string destinationFile)
        {
            using (var outputFile = _fileSystem.File.OpenWrite(destinationFile))
            {
                Console.WriteLine($"Writing file: {destinationFile}");
                _storageClient.DownloadObject(bucket, obj.Name, outputFile);
            }
        }
    }
}