# ReadEndOfStreamingResult

## Location
[src/bin/pg_basebackup/receivelog.c:699-744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.c#L699-L744)

## Overview
Parses the server result set returned when streaming replication reaches an end-of-timeline condition to extract the next timeline ID and starting position.

## Definition

```c
static bool
ReadEndOfStreamingResult(PGresult *res, XLogRecPtr *startpos, uint32 *timeline)
```
## Detailed Description
 is a helper function that processes the result set returned by the PostgreSQL server when streaming replication encounters an end-of-timeline condition. When a timeline ends (typically due to server promotion), the server provides information about the next timeline and where streaming should continue. This function parses a two-column result set containing the next timeline ID and its starting WAL position, validating the format and converting the WAL position from string format (X/X) to internal XLogRecPtr representation.

The function is critical for maintaining continuous replication across timeline transitions in PostgreSQL streaming replication.

## Parameters / Member Variables
- `*res`: PGresult containing the server's end-of-timeline response
- `*startpos`: Output parameter to receive the parsed starting WAL position for the next timeline
- `*timeline`: Output parameter to receive the next timeline ID
## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atoi
  - sscanf
- Called from (representative examples):
  - [ReceiveXlogStream](ReceiveXlogStream.md)

## Notes and Other Information
- Expects exactly one row with two columns: next_tli and next_tli_startpos
- Converts timeline ID from string to integer using atoi()
- Parses WAL position from X/X format using sscanf() with %X/%X pattern
- Combines parsed high and low 32-bit values into a 64-bit XLogRecPtr
- Essential for handling PostgreSQL timeline transitions during streaming replication
- Static function used internally within receivelog.c for streaming operations