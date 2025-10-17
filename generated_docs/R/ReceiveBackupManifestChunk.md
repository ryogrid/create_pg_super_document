# ReceiveBackupManifestChunk

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1717-1734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1717-L1734)

## Overview
A callback function that writes individual chunks of backup manifest data received from the server to a file during the base backup process.

## Definition
```c
static void ReceiveBackupManifestChunk(size_t r, char *copybuf, void *callback_data)
```

## Detailed Description
This function serves as a callback for processing chunks of backup manifest data as they are received from the PostgreSQL server. It is designed to be used with `ReceiveCopyData` to handle streaming data reception. The function writes each received chunk directly to the manifest file and includes comprehensive error handling for write failures.

The function follows the standard callback pattern used throughout PostgreSQL for handling streaming data, where each chunk is processed individually as it arrives. This approach allows for efficient memory usage when dealing with potentially large manifest files.

## Parameters / Member Variables
- `r`: The size (in bytes) of the current chunk of data to write
- `copybuf`: Pointer to the buffer containing the chunk data to be written
- `callback_data`: Void pointer that should contain a `WriteManifestState` structure with file handle and filename information

## Dependencies
- Functions called/Symbols referenced:
  - [WriteManifestState](../W/WriteManifestState.md) (struct cast from callback_data for accessing file state)
  - fwrite (standard C library function to write data to file)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling function for fatal errors)

- Called from (representative examples):
  - [ReceiveBackupManifest](ReceiveBackupManifest.md) (which sets up the callback for ReceiveCopyData)

## Notes and Other Information
- Implements robust error handling by explicitly checking errno and assuming disk space issues when errno is not set by fwrite
- Uses binary write operations suitable for manifest file format
- The callback_data parameter must be properly cast to WriteManifestState* for correct operation  
- Part of the streaming data reception architecture that allows processing large files without loading them entirely into memory
- Error messages include the specific filename for better debugging and troubleshooting

## Simplified Source

```c
static void ReceiveBackupManifestChunk(size_t r, char *copybuf, void *callback_data) {
    WriteManifestState *state = callback_data;

    // Write chunk data to manifest file
    errno = 0;
    if (fwrite(copybuf, r, 1, state->file) != 1) {
        // Assume disk space issue if no specific error
        if (errno == 0)
            errno = ENOSPC;
        pg_fatal("could not write to file \"%s\": %m", state->filename);
    }
}
```