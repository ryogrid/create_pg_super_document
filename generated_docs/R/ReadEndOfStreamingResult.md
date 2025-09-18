# ReadEndOfStreamingResult

## Location
src/bin/pg_basebackup/receivelog.c: 699 - 744

## Overview
Parses the server result set returned when streaming replication reaches an end-of-timeline condition to extract the next timeline ID and starting position.

## Definition


## Detailed Description
 is a helper function that processes the result set returned by the PostgreSQL server when streaming replication encounters an end-of-timeline condition. When a timeline ends (typically due to server promotion), the server provides information about the next timeline and where streaming should continue. This function parses a two-column result set containing the next timeline ID and its starting WAL position, validating the format and converting the WAL position from string format (X/X) to internal XLogRecPtr representation.

The function is critical for maintaining continuous replication across timeline transitions in PostgreSQL streaming replication.

## Parameters / Member Variables
- : PGresult containing the server's end-of-timeline response
- : Output parameter to receive the parsed starting WAL position for the next timeline
- : Output parameter to receive the next timeline ID

## Dependencies
- Functions called/Symbols referenced:
  - PQnfields
  - PQntuples
  - PQgetvalue
  - atoi
  - sscanf
- Called from (representative examples):
  - ReceiveXlogStream

## Notes and Other Information
- Expects exactly one row with two columns: next_tli and next_tli_startpos
- Converts timeline ID from string to integer using atoi()
- Parses WAL position from X/X format using sscanf() with %X/%X pattern
- Combines parsed high and low 32-bit values into a 64-bit XLogRecPtr
- Essential for handling PostgreSQL timeline transitions during streaming replication
- Static function used internally within receivelog.c for streaming operations