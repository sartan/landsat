using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using GCSDownload;
using Google;
using Google.Api.Gax;
using Google.Apis.Download;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using Moq;
using Xunit;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace GCSDownloadTest
{
    public class ParallelDownloaderTest
    {
        private readonly Mock<StorageClient> _storageClient;
        private readonly ParallelDownloader _downloader;
        private readonly IFileSystem _fileSystem;

        private const string BucketName = "a-bucket-name";

        public ParallelDownloaderTest()
        {
            // Using Moq "strict" mode to verify call invocation sequence
            // https://github.com/moq/moq4/issues/748#issuecomment-464338657
            _storageClient = new Mock<StorageClient>(MockBehavior.Strict);
            _fileSystem = new MockFileSystem();
            _downloader = new ParallelDownloader(_storageClient.Object, _fileSystem);
        }

        [Fact]
        public void Download()
        {
            _storageClient
                .Setup(sc => sc.ListObjects(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ListObjectsOptions>()))
                .Returns(new TestPagedEnumerable(new List<Object>
                {
                    new Object {Name = "/a/prefix/a-file"},
                    new Object {Name = "/a/prefix/subdir/b-file"}
                }));

            _storageClient.Setup(sc => sc.DownloadObject(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MockFileStream>(),
                It.IsAny<DownloadObjectOptions>(),
                It.IsAny<IProgress<IDownloadProgress>>()
            ));


            _downloader.Download(BucketName, "/a/prefix", "/destination/path");


            _storageClient.Verify(sc => sc.ListObjects(BucketName, "/a/prefix", null));
            _storageClient.Verify(sc => sc.DownloadObject(BucketName, "/a/prefix/a-file", It.IsAny<MockFileStream>(), null, null));
            _storageClient.Verify(sc => sc.DownloadObject(BucketName, "/a/prefix/subdir/b-file", It.IsAny<MockFileStream>(), null, null));

            // TODO: Verify file contents

            Assert.True(_fileSystem.File.Exists("/destination/path/a/prefix/a-file"));
            Assert.True(_fileSystem.File.Exists("/destination/path/a/prefix/subdir/b-file"));
        }

        [Fact]
        public void Download_RootPrefix()
        {
            _storageClient
                .Setup(sc => sc.ListObjects(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ListObjectsOptions>()))
                .Returns(new TestPagedEnumerable());

            _storageClient.Setup(sc => sc.DownloadObject(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MockFileStream>(),
                It.IsAny<DownloadObjectOptions>(),
                It.IsAny<IProgress<IDownloadProgress>>()
            ));


            _downloader.Download(BucketName, "/", "/destination/path");


            _storageClient.Verify(sc => sc.ListObjects(BucketName, "", null));
        }

        [Fact]
        public void Download_RetriesOnDownloadFailure()
        {
            _storageClient
                .Setup(sc => sc.ListObjects(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ListObjectsOptions>()))
                .Returns(new TestPagedEnumerable(new List<Object>
                {
                    new Object {Name = "/a/prefix/failing-file-to-retry"}
                }));

            _storageClient.Setup(sc => sc.DownloadObject(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MockFileStream>(),
                It.IsAny<DownloadObjectOptions>(),
                It.IsAny<IProgress<IDownloadProgress>>()
            )).Throws<TestGoogleApiException>();


            _downloader.Download(BucketName, "/a/prefix", "/destination/path");


            _storageClient.Verify(
                sc => sc.DownloadObject(BucketName, "/a/prefix/failing-file-to-retry", It.IsAny<MockFileStream>(), null, null),
                Times.Exactly(ParallelDownloader.MaxRetries)
            );
        }

        [Fact]
        public void Download_ContinuesOnDownloadFailure()
        {
            _storageClient.Setup(sc => sc.ListObjects(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ListObjectsOptions>()))
                .Returns(new TestPagedEnumerable(new List<Object>
                {
                    new Object {Name = "/a/prefix/previous-file"},
                    new Object {Name = "/a/prefix/failing-file"},
                    new Object {Name = "/a/prefix/next-file"}
                }));

            var seq = new MockSequence();

            _storageClient.Setup(sc => sc.DownloadObject(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MockFileStream>(),
                It.IsAny<DownloadObjectOptions>(),
                It.IsAny<IProgress<IDownloadProgress>>()
            ));

            _storageClient.InSequence(seq).Setup(sc => sc.DownloadObject(
                BucketName,
                "/a/prefix/failing-file",
                It.IsAny<MockFileStream>(),
                It.IsAny<DownloadObjectOptions>(),
                It.IsAny<IProgress<IDownloadProgress>>()
            )).Throws<TestGoogleApiException>();


            _downloader.Download(BucketName, "/a/prefix", "/destination/path");


            _storageClient.Verify(sc => sc.DownloadObject(BucketName, "/a/prefix/previous-file", It.IsAny<MockFileStream>(), null, null));
            _storageClient.Verify(sc => sc.DownloadObject(BucketName, "/a/prefix/failing-file", It.IsAny<MockFileStream>(), null, null));
            _storageClient.Verify(sc => sc.DownloadObject(BucketName, "/a/prefix/next-file", It.IsAny<MockFileStream>(), null, null));
        }

        [Fact]
        public void Download_FailsOnWrite()
        {
            // TODO
        }

        [Fact]
        public void Download_NoObjects()
        {
            _storageClient
                .Setup(sc => sc.ListObjects(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ListObjectsOptions>()))
                .Returns((PagedEnumerable<Objects, Object>) null);


            _downloader.Download("a-bucket-name", "/a/prefix", "/destination/path");


            _storageClient.Verify(sc => sc.ListObjects("a-bucket-name", "/a/prefix", null));
            _storageClient.VerifyNoOtherCalls();
        }
    }

    internal class TestPagedEnumerable : PagedEnumerable<Objects, Object>
    {
        private readonly ICollection<Object> _objects;

        public TestPagedEnumerable() : this(new List<Object>())
        {
        }

        public TestPagedEnumerable(ICollection<Object> objects) => _objects = objects;

        public override IEnumerator<Object> GetEnumerator() => _objects.GetEnumerator();
    }

    // Needed for Moq because GoogleApiException doesn't have a public parameterless constructor
    internal class TestGoogleApiException : GoogleApiException
    {
        public TestGoogleApiException() : base("testService", "Simulating a failure in a test")
        {
        }
    }
}