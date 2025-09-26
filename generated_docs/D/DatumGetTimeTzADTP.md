# DatumGetTimeTzADTP

## Location
src/include/utils/date.h: 66 - 71

## Overview
DatumGetTimeTzADTP is a static inline function that extracts a TimeTzADT pointer from a PostgreSQL Datum, providing a type-safe conversion mechanism for time-with-timezone values in PostgreSQL's function manager interface.

## Definition

```c
static inline TimeTzADT *
DatumGetTimeTzADTP(Datum X)
```
## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) system, specifically designed to extract time-with-timezone values from Datum objects. It internally delegates to DatumGetPointer() since TimeTzADT structures are passed by reference (as pointers) rather than by value. The function provides type safety and clarity when working with time-with-timezone values in the PostgreSQL backend, ensuring that Datum values are properly interpreted as pointers to TimeTzADT structures.

The function is defined as a static inline function in the header file, meaning it will be inlined at compile time for performance optimization. This is particularly important since time-with-timezone operations are complex and frequently performed throughout PostgreSQL's date/time processing systems.

## Parameters / Member Variables
- : A PostgreSQL Datum containing a pointer to a time-with-timezone value that needs to be converted to TimeTzADT* format

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer (underlying pointer extraction mechanism)
  - TimeTzADT (target structure type)
- Called from (representative examples):
  - JsonEncodeDateTime (JSON encoding of time-with-timezone values)
  - executeDateTimeMethod (JSON path execution for time-with-timezone operations)
  - convert_timevalue_to_scalar (statistics estimation for time-with-timezone values)
  - PG_GETARG_TIMETZADT_P (function argument extraction macro)

## Notes and Other Information
- This function is part of PostgreSQL's type conversion infrastructure for the function manager system
- The implementation uses DatumGetPointer() because TimeTzADT structures contain both time and timezone information, making them too large to pass by value efficiently
- As a static inline function, it provides zero-overhead abstraction for type conversion
- TimeTzADT structures are always passed by reference in PostgreSQL, unlike simpler types like DateADT or TimeADT
- The function is used throughout PostgreSQL's time-with-timezone processing, JSON operations, and statistical analysis
- Located in src/include/utils/date.h, making it available to all components that include this header file
- Works in conjunction with TimeTzADTPGetDatum for bidirectional conversion between Datum and TimeTzADT* types
- The 'P' suffix in the function name indicates it returns a pointer, following PostgreSQL's naming conventions