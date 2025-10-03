# DatumGetIntervalP

## Location
[src/include/utils/timestamp.h:40-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timestamp.h#L40-L45)

## Overview
Converts a PostgreSQL Datum to a pointer to an Interval structure by extracting the pointer value from the Datum.

## Definition
```c
static inline Interval * DatumGetIntervalP(Datum X)
```

## Detailed Description
DatumGetIntervalP is an inline function that extracts a pointer to an Interval structure from a Datum. Unlike timestamp types which are represented as int64 values, Interval is a complex structure that is always passed by reference. This function delegates to DatumGetPointer and casts the result to an Interval pointer. The Interval structure contains fields for time, day, and month components to represent time intervals accurately.

## Parameters / Member Variables
- `X`: The input Datum containing a pointer to the Interval structure to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (implied, though not explicitly shown in references)
  - Interval (pointer type cast)
- Called from (representative examples):
  - [check_timezone](../c/check_timezone.md)
  - [convert_timevalue_to_scalar](../c/convert_timevalue_to_scalar.md)
  - [timestamp_mi](../t/timestamp_mi.md)
  - [in_range_interval_interval](../i/in_range_interval_interval.md)
  - PG_GETARG_INTERVAL_P (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- Interval structures are always passed by reference due to their complex multi-field nature
- Used in timezone validation, statistical functions, timestamp arithmetic, and range operations
- The Interval type represents time spans and can include years, months, days, hours, minutes, seconds, and microseconds
- Location: src/include/utils/timestamp.h:40-45

## Simplified Source

```c
static inline Interval * DatumGetIntervalP(Datum X) {
    // Extract Interval pointer from Datum (pass-by-reference type)
    return (Interval *) DatumGetPointer(X);
}
```