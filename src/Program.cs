using System;
using System.IO.Abstractions;
using Google.Cloud.Storage.V1;
using Microsoft.Extensions.DependencyInjection;

namespace GCSDownload
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: bucket_name prefix download_destination");
                Environment.Exit(-1);
            }

            var startTime = DateTime.Now;

            var bucketName = args[0];
            var prefix = args[1];
            var downloadDir = args[2];

            var serviceProvider = new ServiceCollection()
                .AddScoped<IFileSystem, FileSystem>()
                .AddScoped(sp => StorageClient.Create())
                .AddScoped<IDownloader, ParallelDownloader>()
                .BuildServiceProvider();

            var downloader = serviceProvider.GetService<IDownloader>();

            downloader.Download(
                bucket: bucketName,
                prefix: prefix,
                destination: downloadDir
            );

            var duration = DateTime.Now - startTime;

            Console.WriteLine($"Total duration: {duration.TotalMilliseconds}ms ({duration.TotalSeconds}s)");
        }
    }
}