# tsqueryrecv

## Location
src/backend/utils/adt/tsquery.c: 1227 - 1362

## Overview
Deserializes binary data into a TSQuery structure for PostgreSQL's full-text search functionality.

## Definition
```c
Datum tsqueryrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tsqueryrecv` function is a PostgreSQL binary input function that reconstructs a TSQuery data structure from its binary representation. This function is the counterpart to `tsquerysend` and is used for receiving TSQuery objects from network transmission or binary storage.

The function performs comprehensive validation of the input data including:
- Checking query size limits against MaxAllocSize
- Validating operand weights (must be ≤ 0xF)
- Ensuring operand lengths don't exceed MAXSTRLEN
- Verifying total operand length doesn't exceed MAXSTRPOS
- Validating operator types (OP_NOT, OP_OR, OP_AND, OP_PHRASE)
- Ensuring proper query tree structure

The function processes binary data in prefix notation, allocates appropriate memory, validates all components, and constructs the final TSQuery with proper left-pointer relationships using the `findoprnd` helper function.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention:
- Uses `PG_GETARG_POINTER(0)` to retrieve the StringInfo buffer containing binary data
- Returns a reconstructed TSQuery via `PG_RETURN_TSQUERY()`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER: Extract binary data buffer from function arguments
  - [pq_getmsgint](../p/pq_getmsgint.md): Read integer from binary buffer
  - [pq_getmsgstring](../p/pq_getmsgstring.md): Read string from binary buffer
  - [palloc](../p/palloc.md)/palloc0: Allocate memory
  - [repalloc](../r/repalloc.md): Reallocate memory
  - [pfree](../p/pfree.md): Free memory
  - GETQUERY: Get query items array from TSQuery
  - GETOPERAND: Get operand string data from TSQuery
  - [findoprnd](../f/findoprnd.md): Validate and set up query tree structure
  - INIT_LEGACY_CRC32/COMP_LEGACY_CRC32/FIN_LEGACY_CRC32: Calculate CRC for operands
  - SET_VARSIZE: Set variable-length data size
  - PG_RETURN_TSQUERY: Return TSQuery result

- Called from (representative examples):
  - No direct references found in codebase (likely called via PostgreSQL's type system)

## Notes and Other Information
- This is a standard PostgreSQL binary input function for the TSQuery type
- Performs extensive validation to prevent malformed or malicious input
- Uses CRC32 checksums for operand validation and deduplication
- Temporarily stores operand strings in a separate array before copying to final structure
- The function ensures the reconstructed query tree is well-formed through `findoprnd`
- Located in src/backend/utils/adt/tsquery.c:1227-1362
- Critical for network communication and binary storage of full-text search queries