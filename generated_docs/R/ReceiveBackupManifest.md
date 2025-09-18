# ReceiveBackupManifest

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1698-1716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1698-L1716)

## Overview
Receives the backup manifest file from a PostgreSQL server connection and writes it to a temporary file during the base backup process.

## Definition
```c
static void ReceiveBackupManifest(PGconn *conn)
```

## Detailed Description
This function is responsible for receiving the backup manifest file during a PostgreSQL base backup operation. It creates a temporary file named `backup_manifest.tmp` in the base directory and uses the `ReceiveCopyData` function to stream the manifest data from the server connection. The manifest file contains metadata about the backup, including file checksums, sizes, and other verification information that ensures backup integrity.

The function sets up a `WriteManifestState` structure to track the output file handle and filename, then delegates the actual data reception to `ReceiveCopyData` with `ReceiveBackupManifestChunk` as the callback function for processing each chunk of manifest data.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object used to receive the manifest data from the server

## Dependencies
- Functions called/Symbols referenced:
  - [WriteManifestState](../W/WriteManifestState.md) (struct for maintaining file state)
  - fopen (standard C library function to open the output file)
  - [ReceiveCopyData](ReceiveCopyData.md) (PostgreSQL utility function for receiving streamed data)
  - [ReceiveBackupManifestChunk](ReceiveBackupManifestChunk.md) (callback function for processing manifest chunks)
  - fclose (standard C library function to close the output file)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling function)

- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (main base backup orchestration function)

## Notes and Other Information
- The manifest file is initially created with a `.tmp` extension for atomicity
- File operations use binary mode ("wb") for cross-platform compatibility
- Error handling includes checking for file creation failures with descriptive error messages
- This function is part of the pg_basebackup utility which creates consistent backups of PostgreSQL databases