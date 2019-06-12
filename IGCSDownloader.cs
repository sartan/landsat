namespace landsat
{
    public interface IGcsDownloader
    {
        void Download(string bucket, string prefix, string destination);
    }
}