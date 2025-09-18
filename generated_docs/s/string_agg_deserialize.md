# string_agg_deserialize

## Location
src/backend/utils/adt/varlena.c: 5322 - 5357

## Overview
The deserialize function for PostgreSQL's string_agg() aggregate that converts a bytea representation back into a StringInfo state for parallel worker communication.

## Definition


## Detailed Description
The string_agg_deserialize function serves as the deserialize function for the string_agg aggregate, used in parallel query execution to convert a binary bytea format back into the internal StringInfo state. This function is the counterpart to string_agg_serialize and is essential for reconstructing aggregation state in parallel workers.

Key behavioral aspects:
- Takes a bytea input containing serialized StringInfo state
- Creates a new StringInfo state in the appropriate aggregate context
- Uses PostgreSQL's type receive protocol functions for binary deserialization
- Reconstructs both the cursor field (first delimiter length) and the accumulated string data
- The function is declared as strict, so NULL inputs are not expected
- Ensures the reconstructed state is properly allocated in the aggregate memory context

The deserialization process:
1. Reads the cursor value (4-byte integer) from the binary data
2. Reads the remaining bytes as the accumulated string data
3. Creates a new StringInfo state and populates it with the deserialized data

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing:
  - Arg 0: bytea containing serialized StringInfo state (never NULL due to strict declaration)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (validates aggregate execution context)
  - PG_GETARG_BYTEA_PP (extracts bytea argument)
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md) (initializes read-only buffer for deserialization)
  - [makeStringAggState](../m/makeStringAggState.md) (creates new StringInfo state in aggregate context)
  - [pq_getmsgint](../p/pq_getmsgint.md) (reads 4-byte integer from buffer)
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md) (reads byte array from buffer)
  - appendBinaryStringInfo (appends binary data to StringInfo)
  - [pq_getmsgend](../p/pq_getmsgend.md) (validates complete message consumption)
  - VARDATA_ANY, VARSIZE_ANY_EXHDR (bytea data access macros)

- Called from:
  - PostgreSQL parallel aggregate execution framework (not directly referenced in source)

## Notes and Other Information
- Uses Assert instead of explicit error checking since it's a strict function
- Part of PostgreSQL's parallel aggregation infrastructure for distributed computation
- Must be paired with string_agg_serialize for proper parallel aggregation functionality
- The cursor field restoration is critical for proper delimiter handling in the final function
- Uses read-only StringInfo for efficient binary buffer parsing during deserialization
- Calculates data length by subtracting cursor field size (4 bytes) from total bytea size
- Ensures proper memory context allocation through makeStringAggState
- Validates complete message consumption with pq_getmsgend for data integrity