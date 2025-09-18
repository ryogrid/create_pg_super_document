# tsvectorrecv

## Location
[src/backend/utils/adt/tsvector.c:446-554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L446-L554)

## Overview
The  function deserializes binary data received over the network into a TSVector data structure, performing validation and proper memory management during reconstruction.

## Definition


## Detailed Description
This function is the binary receive function for the TSVector data type, responsible for parsing binary data transmitted via PostgreSQL's binary protocol and reconstructing it into a valid TSVector structure. The function reads the binary format created by : starting with the lexeme count, then for each lexeme reading the null-terminated text, position count, and position data.

The function performs extensive validation during deserialization, checking lexeme lengths against MAXSTRLEN, total data size against MAXSTRPOS, and position counts against MAXNUMPOS. It dynamically allocates and reallocates memory as needed to accommodate the incoming data, ensuring proper alignment for position data structures. The function also maintains lexeme ordering by detecting when sorting is needed and applying qsort with compareentry function when necessary. Position data is validated to ensure positions are in ascending order within each lexeme.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing the binary data buffer (StringInfo)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER: Extract StringInfo buffer from function arguments
  - [pq_getmsgint](../p/pq_getmsgint.md): Read integer from binary message buffer
  - [pq_getmsgstring](../p/pq_getmsgstring.md): Read null-terminated string from binary message buffer
  - [palloc0](../p/palloc0.md): Allocate zero-initialized memory
  - [repalloc](../r/repalloc.md): Reallocate memory with larger size
  - STRPTR: Get pointer to string data in TSVector
  - POSDATAPTR: Get pointer to position data
  - [compareentry](../c/compareentry.md): Compare function for WordEntry sorting
  - qsort_arg: Sort function with custom comparison
  - ARRPTR: Get pointer to word entries array
  - SHORTALIGN: Align to 2-byte boundary
  - SET_VARSIZE: Set variable-length data structure size
  - WEP_GETPOS: Extract position from WordEntryPos
  - PG_RETURN_TSVECTOR: Return TSVector result
- Called from (representative examples):
  - PostgreSQL binary protocol handlers
  - Client-server communication for TSVector data reception

## Notes and Other Information
- Performs comprehensive validation of all incoming data to prevent malformed TSVector creation
- Handles dynamic memory allocation with proper alignment requirements for position data
- Maintains lexeme ordering through conditional sorting when input is not pre-sorted
- Validates position sequences within each lexeme to ensure ascending order
- Uses zero-initialized memory allocation and careful padding for alignment requirements
- Maximum limits enforced: MAXSTRLEN for lexeme length, MAXSTRPOS for total length, MAXNUMPOS for position count
- Memory is reallocated as needed during parsing to accommodate variable-sized data