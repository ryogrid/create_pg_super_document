# timestamp_fastcmp

## Location
src/backend/utils/adt/timestamp.c: 2281 - 2290

## Overview
A fast comparison function for timestamp values optimized for sorting operations, used by PostgreSQL's sort support infrastructure.

## Definition
static int timestamp_fastcmp(Datum x, Datum y, SortSupport ssup)

## Detailed Description
This is a specialized comparison function designed for high-performance sorting of timestamp values. It operates directly on Datum values (PostgreSQL's generic data container) and is used by PostgreSQL's sort support framework to optimize sorting operations on timestamp columns. The function converts the Datum parameters to Timestamp values and delegates to the internal comparison logic. Unlike the standard comparison functions, this is a static function optimized for internal sorting operations and supports both timestamp and timestamptz data types.

## Parameters / Member Variables
- x: First Datum containing a timestamp value to compare
- y: Second Datum containing a timestamp value to compare  
- ssup: SortSupport structure (unused in this implementation but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetTimestamp (macro to extract timestamp from Datum)
  - [timestamp_cmp_internal](timestamp_cmp_internal.md) (internal comparison function)
  - SortSupport (type for sort support structure)
- Called from (representative examples):
  - [timestamp_sortsupport](timestamp_sortsupport.md) (in src/backend/utils/adt/timestamp.c:2303)

## Notes and Other Information
- This function is used for both timestamp and timestamptz data types as noted in the comment
- Optimized for sorting performance by operating directly on Datum values
- Part of PostgreSQL's sort support infrastructure for efficient sorting operations
- Static function, only accessible within the timestamp.c compilation unit
- Located in src/backend/utils/adt/timestamp.c:2281-2290
- Returns standard comparison result: -1 for less than, 0 for equal, 1 for greater than