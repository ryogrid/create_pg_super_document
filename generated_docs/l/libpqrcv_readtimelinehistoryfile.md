# libpqrcv_readtimelinehistoryfile

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:732-785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L732-L785)

## Overview
Fetches the timeline history file for a specified timeline ID from the primary server during WAL replication.

## Definition

```c
static void
libpqrcv_readtimelinehistoryfile(WalReceiverConn *conn,
								 TimeLineID tli, char **filename,
								 char **content, int *len)
```
## Detailed Description
This function sends a TIMELINE_HISTORY command to the primary server to retrieve the timeline history file for a given timeline ID. The timeline history file contains information about timeline switches and is crucial for WAL replication consistency. The function validates the response format and extracts both the filename and content of the history file.

The function uses the libpq protocol to send the command and expects exactly one tuple with two fields in response: the filename and the binary content of the timeline history file.

## Parameters / Member Variables
- `*conn`: WAL receiver connection object containing the stream connection to the primary
- `tli`: Timeline ID for which to fetch the history file
- `**filename`: Output parameter that receives a palloc'd copy of the history filename
- `**content`: Output parameter that receives a palloc'd copy of the file content
- `*len`: Output parameter that receives the length of the content in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [libpqrcv_PQexec](libpqrcv_PQexec.md) (for sending the TIMELINE_HISTORY command)
  - [PQresultStatus](../P/PQresultStatus.md) (for checking command result status)
  - [PQnfields](../P/PQnfields.md) (for validating response structure)
  - [PQntuples](../P/PQntuples.md) (for validating response structure) 
  - [PQgetvalue](../P/PQgetvalue.md) (for extracting result data)
  - [PQgetlength](../P/PQgetlength.md) (for getting content length)
  - [PQclear](../P/PQclear.md) (for cleaning up result)
  - [pstrdup](../p/pstrdup.md) (for copying filename)
  - [palloc](../p/palloc.md) (for allocating content buffer)
  - [pchomp](../p/pchomp.md) (for error message formatting)
- Called from (representative examples):
  - Used internally by WAL receiver functions during timeline history retrieval

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- The function asserts that the connection is not for logical replication (conn->logical must be false)
- Error handling includes specific protocol violation errors for malformed responses
- Memory allocation for both filename and content is done using PostgreSQL's memory management functions
- The function expects exactly 1 tuple with 2 fields, otherwise it raises a protocol violation error