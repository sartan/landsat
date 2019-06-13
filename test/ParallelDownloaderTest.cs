using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using GCSDownload;
using Google.Api.Gax;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using Moq;
using Xunit;

namespace GCSDownloadTest
{
    public class ParallelDownloaderTest
    {
        private readonly Mock<StorageClient> _storageClient;
        private readonly ParallelDownloader _downloader;
        private readonly IFileSystem _fileSystem;

        public ParallelDownloaderTest()
        {
            _storageClient = new Mock<StorageClient>();
            _fileSystem = new MockFileSystem();
            _downloader = new ParallelDownloader(_storageClient.Object, _fileSystem);
        }

        [Fact]
        public void Download()
        {
            const string bucketName = "a-bucket-name";

            _storageClient
                .Setup(sc => sc.ListObjects(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ListObjectsOptions>()))
                .Returns(new TestPagedEnumerable(new List<Object>
                {
                    new Object {Name = "/a/prefix/a-file"},
                    new Object {Name = "/a/prefix/subdir/b-file"}
                }));

            _downloader.Download(bucketName, "/a/prefix", "/destination/path");

            _storageClient.Verify(sc => sc.ListObjects(bucketName, "/a/prefix", null));
            // TODO: Verify file contents
            _storageClient.Verify(sc => sc.DownloadObject(bucketName, "/a/prefix/a-file", It.IsAny<MockFileStream>(), null, null));
            _storageClient.Verify(sc => sc.DownloadObject(bucketName, "/a/prefix/subdir/b-file", It.IsAny<MockFileStream>(), null, null));

            Assert.True(_fileSystem.File.Exists("/destination/path/a/prefix/a-file"));
            Assert.True(_fileSystem.File.Exists("/destination/path/a/prefix/subdir/b-file"));
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

        public TestPagedEnumerable(ICollection<Object> objects) => _objects = objects;

        public override IEnumerator<Object> GetEnumerator() => _objects.GetEnumerator();
    }
}