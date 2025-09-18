# ReceiveCopyData

## Location
src/bin/pg_basebackup/pg_basebackup.c: 1014 - 1060

## Overview
A core function that manages the reception of PostgreSQL COPY protocol data streams and dispatches each received chunk to a user-provided callback function for processing.

## Definition
```c
static void ReceiveCopyData(PGconn *conn, WriteDataCallback callback, void *callback_data)
```

## Detailed Description
This function implements the client-side handling of PostgreSQL COPY OUT protocol for receiving streaming data from the server. It establishes the COPY data stream, then enters a loop to continuously receive data chunks until the stream is complete. Each received chunk is immediately passed to the provided callback function along with associated callback data.

The function handles three possible return values from PQgetCopyData():
- Positive values indicating successful data reception with the returned byte count
- -1 indicating end of stream (normal termination)
- -2 indicating an error condition

The function also includes safety checks for background process termination and ensures proper memory management by freeing each received buffer after callback processing.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle through which to receive the COPY data stream
- `callback`: Function pointer to WriteDataCallback that will process each received data chunk
- `callback_data`: Opaque pointer passed through to the callback function for maintaining callback state

## Dependencies
- Functions called/Symbols referenced:
  - PQgetResult (libpq function to get query result)
  - PQresultStatus (libpq function to check result status)
  - PQclear (libpq function to free result memory)
  - PQgetCopyData (libpq function to receive COPY data chunks)
  - PQerrorMessage (libpq function to get error message)
  - PQfreemem (libpq function to free allocated memory)
  - pg_fatal (PostgreSQL error reporting function)
  - PGRES_COPY_OUT (libpq constant for COPY OUT result status)
- Called from (representative examples):
  - ReceiveArchiveStream
  - ReceiveTarFile
  - ReceiveBackupManifest
  - ReceiveBackupManifestInMemory

## Notes and Other Information
- This is a static function with internal linkage within pg_basebackup.c
- The function implements proper PostgreSQL COPY protocol handling with error checking
- Memory management is handled correctly with PQfreemem() calls after each chunk
- Includes monitoring for background process termination via bgchild_exited flag
- The callback pattern allows for flexible processing of received data without coupling this function to specific data handling logic
- Essential for pg_basebackup operations that receive large data streams from the server
- Error conditions result in program termination via pg_fatal() calls