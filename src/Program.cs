using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using Microsoft.Extensions.DependencyInjection;
using Object = System.Object;

namespace GCSDownload
{
    class Program
    {
        static void Main(string[] args)
        {
            // If/else dir cache:                114188.51ms, 189050.292ms
            // If dir cache:                      38039.178ms
            // No dir cache:                      35979.628ms, 37687.228ms
            // No dir cache, no output:           34946.14ms
            // Async:                            140320.256ms, 33241.146ms, 38227.983ms, 35930.7ms, 37270.552ms
            // Parallel:                          46654.125ms, 42487.543ms, 93248.085ms, 45494.98ms, 30764.773ms, 66513.513ms
            var startTime = DateTime.Now;

            const string bucketName = "gcp-public-data-landsat";
            const string prefix = "LC08/01/001/027/LC08_L1TP_001027_20130606_20170504_01_T1";
            const string downloadDir = "./data";

            var serviceProvider = new ServiceCollection()
                .AddScoped(sp => StorageClient.Create())
                .AddScoped<IGcsDownloader, ParallelGcsDownloader>()
                .BuildServiceProvider();

            var downloader = serviceProvider.GetService<IGcsDownloader>();

            downloader.Download(
                bucket: bucketName,
                prefix: prefix,
                destination: downloadDir
            );

            var duration = DateTime.Now - startTime;

            Console.WriteLine($"Total duration: {duration.TotalMilliseconds}ms");
        }
    }
}