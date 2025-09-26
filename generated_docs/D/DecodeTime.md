# DecodeTime

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1435-1499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1435-L1499)

## Overview
DecodeTime is a function that parses time strings with delimiters and converts them into structured time components for timestamp processing.

## Definition

```c
int
DecodeTime(char *str, int *tmask, struct tm *tm, fsec_t *fsec)
```
## Detailed Description
DecodeTime is specifically designed for timestamp processing and serves as a wrapper around DecodeTimeCommon. It parses time strings that include delimiters (like colons) and extracts hour, minute, second, and microsecond components. The function validates that the hour value fits within integer range and populates the provided pg_tm structure with the decoded time components. On success, it returns 0; on error, it returns a DTERR error code.

## Parameters / Member Variables
- : Input time string to be decoded
- : Format mask indicating expected time components
- : Range validation parameter
- : Output parameter receiving mask of successfully decoded time fields
- : Output pg_tm structure to receive hour, minute, and second values
- : Output parameter to receive microseconds component

## Dependencies
- Functions called/Symbols referenced:
  - [DecodeTimeCommon](DecodeTimeCommon.md)
  - fsec_t (type)
  - [pg_tm](../p/pg_tm.md) (type)
  - [pg_itm](../p/pg_itm.md) (type)
  - DTERR_FIELD_OVERFLOW (constant)
- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md)
  - [DecodeTimeOnly](DecodeTimeOnly.md)
  - [DecodeInterval](DecodeInterval.md) (in ECPG)

## Notes and Other Information
- This is a static function specific to timestamp processing
- Performs overflow checking on hour values before assignment
- Uses intermediate pg_itm structure internally via DecodeTimeCommon
- Part of PostgreSQL's datetime parsing infrastructure
- Has corresponding implementations in ECPG client library