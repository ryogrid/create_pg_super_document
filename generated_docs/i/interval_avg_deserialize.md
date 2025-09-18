# interval_avg_deserialize

## Location
src/backend/utils/adt/timestamp.c: 4106 - 4148

## Overview
Deserializes bytea back into IntervalAggState for interval aggregates, reconstructing the aggregation state from serialized binary data.

## Definition


## Detailed Description
This function reconstructs an IntervalAggState structure from a serialized bytea format using PostgreSQL's binary protocol functions. It allocates memory for a new IntervalAggState structure and deserializes all fields in the same order they were serialized: the count of finite values (N), the summed interval components (time, day, month), and the counts of positive and negative infinity values. The function uses PostgreSQL's standard message reception infrastructure with a read-only StringInfo buffer initialized from the bytea input. It includes validation to ensure proper aggregate context and verifies complete message consumption.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - ARG 0: bytea containing serialized IntervalAggState

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext (validates aggregate context)
  - PG_GETARG_BYTEA_PP (macro for retrieving bytea arguments)
  - initReadOnlyStringInfo (initializes read buffer from bytea)
  - VARDATA_ANY (extracts data from bytea)
  - VARSIZE_ANY_EXHDR (gets bytea size excluding header)
  - palloc0 (allocates zeroed memory)
  - pq_getmsgint64 (deserializes 64-bit integers)
  - pq_getmsgint (deserializes 32-bit integers)
  - pq_getmsgend (validates complete message consumption)
  - PG_RETURN_POINTER (macro for returning pointer values)
  - elog (error logging function)
- Called from (representative examples):
  - PostgreSQL parallel aggregation system (registered as deserialize function)

## Notes and Other Information
- Essential counterpart to interval_avg_serialize for parallel query execution
- Deserialization order must match serialization: N, sumX.time, sumX.day, sumX.month, pInfcount, nInfcount
- Allocates new memory using palloc0 ensuring all fields are zero-initialized
- Validates complete consumption of serialized data to detect format errors
- Includes runtime validation to prevent misuse outside aggregate contexts
- Memory allocated in current memory context will be automatically cleaned up by PostgreSQL