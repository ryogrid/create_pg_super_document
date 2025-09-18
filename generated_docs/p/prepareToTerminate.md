# prepareToTerminate

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:1042-1069](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L1042-L1069)

## Overview
A function that gracefully notifies the PostgreSQL server about the upcoming termination of the logical replication stream and logs the termination reason.

## Definition


## Detailed Description
The  function handles the graceful shutdown of a logical replication stream by notifying the server that the client is about to disconnect. It sends a copy end message to the server using  and flushes the connection to ensure the message is transmitted. The function also provides informative logging when verbose mode is enabled, displaying different messages based on the reason for termination (signal received, keepalive reached end position, or WAL record reached end position). The function is designed to be non-blocking and fault-tolerant - it doesn't wait for responses or retry on failures, as the primary goal is to attempt clean disconnection before termination.

## Parameters / Member Variables
- : A pointer to the PostgreSQL connection object for server communication
- : The final XLogRecPtr position that was reached before termination
- : An enum value of type StreamStopReason indicating why the stream is being stopped
- : The XLogRecPtr position of the specific WAL record that triggered termination (used with STREAM_STOP_END_OF_WAL)

## Dependencies
- Functions called/Symbols referenced:
  - [PQputCopyEnd](../P/PQputCopyEnd.md) (sends copy end message to server)
  - [PQflush](../P/PQflush.md) (flushes connection buffer)
  - pg_log_info (logging function for informational messages)
  - XLogRecPtrIsInvalid (checks if LSN is invalid)
  - StreamStopReason enum values (STREAM_STOP_SIGNAL, STREAM_STOP_KEEPALIVE, STREAM_STOP_END_OF_WAL, STREAM_STOP_NONE)
- Called from (representative examples):
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (when terminating logical replication stream)

## Notes and Other Information
- This is a static function, meaning it's only visible within its compilation unit
- The function deliberately ignores return values from PQputCopyEnd and PQflush to avoid hanging during shutdown
- Verbose logging provides different messages based on the termination reason for debugging and monitoring purposes
- Used specifically in the pg_recvlogical utility for logical replication WAL streaming
- The function includes assertions to catch programming errors in debug builds (Assert calls)
- Designed to be called as part of the cleanup process when stopping logical replication streaming