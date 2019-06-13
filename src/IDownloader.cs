namespace GCSDownload
{
    public interface IDownloader
    {
        void Download(string bucket, string prefix, string destination);
    }
}