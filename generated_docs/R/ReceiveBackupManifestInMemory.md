# ReceiveBackupManifestInMemory

## Location
[src/bin/pg_basebackup/pg_basebackup.c:1735-1743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L1735-L1743)

## Overview
Receives the backup manifest file from a PostgreSQL server connection and stores it in memory using a PQExpBuffer rather than writing it to a file.

## Definition
```c
static void ReceiveBackupManifestInMemory(PGconn *conn, PQExpBuffer buf)
```

## Detailed Description
This function provides an alternative approach to `ReceiveBackupManifest` by storing the backup manifest data in memory rather than writing it directly to a file. It uses PostgreSQLs `PQExpBuffer` data structure to dynamically accumulate the manifest content as it is received from the server connection.

The function leverages the same streaming data reception mechanism as its file-based counterpart but delegates chunk processing to `ReceiveBackupManifestInMemoryChunk`, which appends data to the memory buffer instead of writing to disk. This approach is useful when the manifest data needs to be processed or manipulated before being written to storage, or when working within memory constraints.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object used to receive the manifest data from the server
- `buf`: PQExpBuffer structure that will store the received manifest data in memory

## Dependencies
- Functions called/Symbols referenced:
  - [ReceiveCopyData](ReceiveCopyData.md) (PostgreSQL utility function for receiving streamed data)
  - [ReceiveBackupManifestInMemoryChunk](ReceiveBackupManifestInMemoryChunk.md) (callback function for processing manifest chunks into memory)

- Called from (representative examples):
  - [ReceiveTarFile](ReceiveTarFile.md) (when manifest data needs to be processed in memory during tar file reception)

## Notes and Other Information
- Uses PQExpBuffer for efficient dynamic string/binary data management
- Provides memory-based alternative to file-based manifest reception
- Particularly useful in scenarios where manifest data requires processing before storage
- Part of the flexible backup manifest handling system in pg_basebackup utility
- The PQExpBuffer automatically handles memory allocation and reallocation as data is appended

## Simplified Source

```c
static void ReceiveBackupManifestInMemory(PGconn *conn, PQExpBuffer buf) {
    // Receive manifest data into memory buffer using callback
    ReceiveCopyData(conn, ReceiveBackupManifestInMemoryChunk, buf);
}
```