# timestamp_recv

## Location
[src/backend/utils/adt/timestamp.c:258-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L258-L290)

## Overview
Converts external binary format data to PostgreSQL timestamp data type during deserialization from network or storage.

## Definition
```c
Datum timestamp_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamp_recv` function is part of PostgreSQL's binary I/O interface for the timestamp data type. It reads binary representation of timestamp data from a StringInfo buffer and converts it to PostgreSQL's internal timestamp format. The function performs range validation to ensure the timestamp value is within acceptable bounds and applies any type modifier constraints.

The function extracts a 64-bit integer from the input buffer using `pq_getmsgint64`, performs range checking by attempting to convert it to broken-down time format, and validates that the timestamp falls within PostgreSQL's supported range. Any type modifier constraints are then applied before returning the final timestamp value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (StringInfo buf): Input buffer containing binary timestamp data
- `PG_FUNCTION_ARGS[1]` (Oid typelem): Element type OID (unused, marked with NOT_USED)  
- `PG_FUNCTION_ARGS[2]` (int32 typmod): Type modifier specifying precision constraints

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - TIMESTAMP_NOT_FINITE
  - [timestamp2tm](timestamp2tm.md)
  - IS_VALID_TIMESTAMP
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md)
  - PG_RETURN_TIMESTAMP
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
- This function is part of PostgreSQL's type system binary I/O interface
- [Range](../R/Range.md) validation ensures timestamps are within PostgreSQL's supported range (4713 BC to 294276 AD)
- Special handling for non-finite timestamp values (infinity, -infinity)
- Type modifier adjustment handles precision constraints for fractional seconds
- Located in src/backend/utils/adt/timestamp.c:258-290