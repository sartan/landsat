# Google Cloud Storage Downloader

Downloads contents of a GCS bucket to a specified local path.

## Testing
`dotnet test`
## Running
1. Generate [service account file](https://console.cloud.google.com/apis/credentials)
1. Set environment variable pointing to the key
    - `$env:GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account_file.json"` (PowerShell)
    - `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account_file.json"` (Bash)
1. Run the app: `dotnet run`