# DatumGetDateADT

## Location
src/include/utils/date.h: 54 - 59

## Overview
DatumGetDateADT is a static inline function that extracts a DateADT value from a PostgreSQL Datum, providing a type-safe conversion mechanism for date values in PostgreSQL's function manager interface.

## Definition


## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) system, specifically designed to extract date values from Datum objects. It internally delegates to DatumGetInt32() since DateADT is fundamentally represented as a 32-bit integer in PostgreSQL. The function provides type safety and clarity when working with date values in the PostgreSQL backend, ensuring that Datum values are properly interpreted as DateADT types.

The function is defined as a static inline function in the header file, meaning it will be inlined at compile time for performance optimization, as this is a frequently used conversion operation throughout the PostgreSQL codebase.

## Parameters / Member Variables
- : A PostgreSQL Datum containing a date value that needs to be converted to DateADT format

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt32 (underlying conversion mechanism)
  - DateADT (target type for conversion)
- Called from (representative examples):
  - JsonEncodeDateTime (JSON encoding operations)
  - executeDateTimeMethod (JSON path execution)
  - compareDatetime (date comparison operations)
  - daterange_canonical (range type operations)
  - convert_timevalue_to_scalar (statistics estimation)
  - map_sql_value_to_xml_value (XML conversion)
  - PG_GETARG_DATEADT (function argument extraction macro)

## Notes and Other Information
- This function is part of PostgreSQL's type conversion infrastructure for the function manager system
- The implementation leverages the fact that DateADT is internally represented as a 32-bit integer
- As a static inline function, it provides zero-overhead abstraction for type conversion
- The function is heavily used throughout PostgreSQL's date/time processing, JSON operations, range types, and XML functionality
- Located in src/include/utils/date.h, making it available to all components that include this header file