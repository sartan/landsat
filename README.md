# Google Cloud Storage Downloader

Downloads contents of a GCS bucket to a specified local path.

## Testing
`dotnet test`
## Running
1. Generate [service account file](https://console.cloud.google.com/apis/credentials)
1. Set environment variable pointing to the key
    - `$env:GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account_file.json"` (PowerShell)
    - `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account_file.json"` (Bash)
1. Run the app:
    ```
    cd src
    dotnet run -- bucket-name prefix download-directory
    ```

**Example usage**

Download contents of the [LM01](https://console.cloud.google.com/storage/browser/gcp-public-data-landsat/LM01/)
directory of the [Landsat dataset](https://cloud.google.com/storage/docs/public-datasets/landsat) to `landsat-data`
folder in your home directory.

```cd src
git clone git@github.com:sartan/landsat.git
cd landsat/src
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_file.json
dotnet run -- gcp-public-data-landsat LM01/ ~/landsat-data
```