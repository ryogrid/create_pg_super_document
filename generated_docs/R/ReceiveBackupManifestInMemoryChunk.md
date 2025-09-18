# ReceiveBackupManifestInMemoryChunk

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1744 - 1752

## Overview
A callback function that appends individual chunks of backup manifest data received from the server to a memory buffer during the base backup process.

## Definition
```c
static void ReceiveBackupManifestInMemoryChunk(size_t r, char *copybuf, void *callback_data)
```

## Detailed Description
This function serves as the memory-based counterpart to `ReceiveBackupManifestChunk`, designed to accumulate backup manifest data in memory rather than writing it directly to a file. It acts as a callback for the `ReceiveCopyData` function, processing each chunk of manifest data as it arrives from the PostgreSQL server.

The function is remarkably simple and efficient, using PostgreSQLs `PQExpBuffer` API to append each received chunk to a dynamically growing buffer. This approach provides automatic memory management and efficient string concatenation, making it ideal for scenarios where manifest data needs to be processed in memory before final storage or transmission.

## Parameters / Member Variables
- `r`: The size (in bytes) of the current chunk of data to append
- `copybuf`: Pointer to the buffer containing the chunk data to be appended
- `callback_data`: Void pointer that should contain a `PQExpBuffer` structure for storing the accumulated manifest data

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBuffer (PostgreSQL utility function for appending data to PQExpBuffer)
  - PQExpBuffer (PostgreSQL dynamic string buffer structure, cast from callback_data)

- Called from (representative examples):
  - ReceiveBackupManifestInMemory (which sets up the callback for ReceiveCopyData)

## Notes and Other Information
- Provides a clean, memory-based alternative to file-based manifest chunk processing
- Relies on PQExpBuffers automatic memory management for efficient data accumulation
- No explicit error handling is needed as appendPQExpBuffer handles memory allocation failures internally
- The callback_data parameter must be properly cast to PQExpBuffer for correct operation
- Part of the streaming data architecture that supports both file-based and memory-based manifest processing
- Extremely lightweight implementation focused solely on data accumulation