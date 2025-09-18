# interval_avg_serialize

## Location
[src/backend/utils/adt/timestamp.c:4068-4105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4068-L4105)

## Overview
Serializes IntervalAggState for interval aggregates into a bytea format for transmission across PostgreSQL processes in parallel aggregation.

## Definition


## Detailed Description
This function converts an IntervalAggState structure into a serialized bytea format using PostgreSQL's binary protocol functions. It serializes all fields of the aggregation state including the count of finite values (N), the summed interval components (time, day, month), and the counts of positive and negative infinity values. The function includes a safety check to ensure it's only called within an aggregate context. The serialized format uses PostgreSQL's standard binary protocol with 64-bit integers for counts and time components, and 32-bit integers for day and month components.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - ARG 0: IntervalAggState pointer to serialize

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (validates aggregate context)
  - PG_GETARG_POINTER (macro for retrieving pointer arguments)
  - [pq_begintypsend](../p/pq_begintypsend.md) (begins binary serialization)
  - [pq_sendint64](../p/pq_sendint64.md) (serializes 64-bit integers)
  - [pq_sendint32](../p/pq_sendint32.md) (serializes 32-bit integers)
  - [pq_endtypsend](../p/pq_endtypsend.md) (completes binary serialization)
  - PG_RETURN_BYTEA_P (macro for returning bytea values)
  - elog (error logging function)
- Called from (representative examples):
  - PostgreSQL parallel aggregation system (registered as serialize function)

## Notes and Other Information
- Critical for PostgreSQL's parallel query execution enabling state transfer between processes
- Uses PostgreSQL's portable binary format ensuring cross-platform compatibility
- Serialization order: N, sumX.time, sumX.day, sumX.month, pInfcount, nInfcount
- Includes runtime validation to prevent misuse outside aggregate contexts
- The serialized bytea can be transmitted over network connections or stored for later deserialization