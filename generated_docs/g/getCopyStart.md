# getCopyStart

## Location
[src/interfaces/libpq/fe-protocol3.c:1554-1609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1554-L1609)

## Overview
Processes CopyInResponse, CopyOutResponse, or CopyBothResponse messages from the PostgreSQL server as part of the COPY protocol implementation in libpq.

## Definition

```c
static int
getCopyStart(PGconn *conn, ExecStatusType copytype)
```
## Detailed Description
The getCopyStart function handles the initial phase of PostgreSQL's COPY protocol by parsing the server's response message that indicates the start of a COPY operation. The function reads the binary/text format flag, the number of fields, and the format codes for each field from the network stream. It creates a PGresult structure to store this metadata information which will be used throughout the COPY operation.

The function assumes that parseInput has already read the message type and length before this function is called. It processes the remaining message payload to extract COPY-specific parameters and prepares the connection state for subsequent COPY data handling.

## Parameters / Member Variables
- : PostgreSQL connection object containing the network stream and connection state
- : The type of COPY operation (PGRES_COPY_IN, PGRES_COPY_OUT, or PGRES_COPY_BOTH) determined from the message type

## Dependencies
- Functions called/Symbols referenced:
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [pqGetc](../p/pqGetc.md)  
  - [pqGetInt](../p/pqGetInt.md)
  - [pqResultAlloc](../p/pqResultAlloc.md)
  - MemSet
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md) (when processing CopyInResponse, CopyOutResponse, or CopyBothResponse messages)

## Notes and Other Information
- Returns 0 on success, EOF on failure
- Sets conn->result to the newly created PGresult on success
- Handles both binary and text format COPY operations
- Allocates memory for attribute descriptors based on the number of fields
- Properly handles signed/unsigned integer conversion for format codes
- On failure, cleans up allocated resources before returning