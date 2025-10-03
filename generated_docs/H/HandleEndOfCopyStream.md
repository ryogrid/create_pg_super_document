# HandleEndOfCopyStream

## Location
[src/bin/pg_basebackup/receivelog.c:1171-1210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L1171-L1210)

## Overview
HandleEndOfCopyStream handles the graceful termination of a streaming replication copy stream, closing files and sending appropriate protocol messages.

## Definition

```c
static PGresult *
HandleEndOfCopyStream(PGconn *conn, StreamCtl *stream, char *copybuf,
					  XLogRecPtr blockpos, XLogRecPtr *stoppos)
```
## Detailed Description
This function manages the orderly shutdown of a streaming replication session when the server closes its end of the copy stream. It ensures that any open WAL file is properly closed and that the copy protocol is completed correctly by sending a copy-end packet if necessary. The function handles different scenarios: normal termination where the client closes the copy stream gracefully, and error conditions where the server may have terminated due to an error. It also cleans up allocated memory and sets the final stop position for the streaming session.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection object for protocol communication
- `*stream`: StreamCtl structure containing streaming configuration and WAL method
- `*copybuf`: Buffer that needs to be freed during cleanup
- `blockpos`: Current block position when the stream ended
- `*stoppos`: Pointer to store the final stopping position
## Dependencies
- Functions called/Symbols referenced:
  - [PQgetResult](../P/PQgetResult.md)
  - [close_walfile](../c/close_walfile.md)
  - [PQclear](../P/PQclear.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQputCopyEnd](../P/PQputCopyEnd.md)
  - [PQflush](../P/PQflush.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQfreemem](../P/PQfreemem.md)
  - pg_log_error
- Called from (representative examples):
  - [HandleCopyStream](HandleCopyStream.md)

## Notes and Other Information
- Returns PGresult pointer from the final server response, or NULL on error
- Only attempts graceful shutdown if still_sending flag is true
- Handles PGRES_COPY_IN state by sending appropriate copy-end packet
- Sets still_sending to false to indicate streaming has stopped
- Records final stop position for caller reference
- Critical for ensuring proper cleanup and protocol compliance during streaming termination
- Must handle both normal shutdown and error conditions gracefully