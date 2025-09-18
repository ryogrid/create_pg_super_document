# interval_part

## Location
src/backend/utils/adt/timestamp.c: 6143 - 6148

## Overview
PostgreSQL built-in function wrapper that extracts specified time components from interval values, returning results as float8 values.

## Definition


## Detailed Description
This function serves as the public interface for the SQL interval_part() function. It is a simple wrapper around interval_part_common() that specifies float8 return type (retnumeric=false). The function takes two arguments through PG_FUNCTION_ARGS: a text unit specification and an interval value, then delegates all processing to the common implementation.

This function is typically called directly from SQL queries using the interval_part() function syntax, providing floating-point precision results suitable for most use cases where exact decimal precision is not required.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - units: Text string specifying the time unit to extract (e.g., 'day', 'hour', 'month')
  - interval: The interval value to extract the component from

## Dependencies
- Functions called/Symbols referenced:
  - [interval_part_common](interval_part_common.md) (with retnumeric=false for float8 return type)
- Called from:
  - SQL queries using interval_part() function

## Notes and Other Information
This function is registered in PostgreSQL's system catalog as a built-in function and can be invoked directly from SQL. It provides the float8 variant of interval component extraction, while extract_interval() provides the numeric variant. The choice between them depends on whether exact decimal precision (numeric) or floating-point performance (float8) is preferred for the specific use case.