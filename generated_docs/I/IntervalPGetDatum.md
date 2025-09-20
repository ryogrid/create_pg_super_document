# IntervalPGetDatum

## Location
[src/include/utils/timestamp.h:58-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timestamp.h#L58-L62)

## Overview
IntervalPGetDatum is an inline utility function that converts an Interval pointer to a PostgreSQL Datum for use in the internal function call interface.

## Definition

```c
static inline Datum
IntervalPGetDatum(const Interval *X)
```
## Detailed Description
This function serves as a type-safe wrapper around PointerGetDatum specifically for Interval data types. It takes a pointer to an Interval structure and converts it to a Datum, which is PostgreSQL's universal data type used for passing values in the function call interface. The function is implemented as a simple inline wrapper that calls PointerGetDatum internally, providing type safety and semantic clarity when working with interval values.

The function is part of PostgreSQL's type conversion system that allows different data types to be uniformly handled as Datum values in the function call protocol. This is essential for the extensible function system where functions can accept and return various data types through a common interface.

## Parameters / Member Variables
- : A const pointer to an Interval structure that will be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md) (internal conversion function)
  - Interval (data type structure)
- Called from (representative examples):
  - PG_STAT_GET_WAL_SENDERS_COLS (WAL sender statistics)
  - [in_range_date_interval](../i/in_range_date_interval.md) (date range operations)
  - [pg_timezone_abbrevs](../p/pg_timezone_abbrevs.md) (timezone abbreviation functions)
  - [pg_timezone_names](../p/pg_timezone_names.md) (timezone name functions) 
  - [timestamp_mi](../t/timestamp_mi.md) (timestamp subtraction)
  - [in_range_timestamp_interval](../i/in_range_timestamp_interval.md) (timestamp range operations)
  - [in_range_interval_interval](../i/in_range_interval_interval.md) (interval range operations)
  - [interval_avg](../i/interval_avg.md) (interval averaging)
  - PG_RETURN_INTERVAL_P (return macro)

## Notes and Other Information
- This function is defined as a static inline function in src/include/utils/timestamp.h:58-62
- It's commonly used with the PG_RETURN_INTERVAL_P macro for returning interval values from PostgreSQL functions
- The function provides type safety by accepting specifically Interval pointers rather than generic void pointers
- As an inline function, it has minimal runtime overhead while providing better type checking than direct PointerGetDatum calls
- This follows PostgreSQL's pattern of having type-specific GetDatum functions for major data types