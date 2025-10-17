# extract_interval

## Location
[src/backend/utils/adt/timestamp.c:6149-6163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6149-L6163)

## Overview
PostgreSQL built-in function wrapper that extracts specified time components from interval values, returning results as exact numeric values.

## Definition

```c
struct pg_tm tm;
```
## Detailed Description
This function serves as the public interface for the SQL EXTRACT() function when applied to interval data types. It is a simple wrapper around interval_part_common() that specifies numeric return type (retnumeric=true). The function takes two arguments through PG_FUNCTION_ARGS: a text unit specification and an interval value, then delegates all processing to the common implementation.

This function is typically called from SQL queries using the EXTRACT(unit FROM interval) syntax, providing exact decimal precision results that avoid the rounding errors inherent in floating-point arithmetic. This makes it ideal for applications requiring precise calculations or when exact reproducibility is important.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - units: Text string specifying the time unit to extract (e.g., 'second', 'minute', 'epoch')
  - interval: The interval value to extract the component from

## Dependencies
- Functions called/Symbols referenced:
  - [interval_part_common](../i/interval_part_common.md) (with retnumeric=true for numeric return type)
- Called from:
  - SQL queries using EXTRACT(unit FROM interval) syntax

## Notes and Other Information
This function is registered in PostgreSQL's system catalog as a built-in function and provides the numeric variant of interval component extraction. It complements interval_part() which returns float8 values. The EXTRACT syntax is SQL standard-compliant and is preferred in applications where exact decimal arithmetic is required, such as financial calculations or when precise time differences must be maintained without floating-point approximations.

## Simplified Source

```c
Datum extract_interval(PG_FUNCTION_ARGS) {
    // Extract interval parts returning numeric values for precision
    return interval_part_common(fcinfo, true);
}
```