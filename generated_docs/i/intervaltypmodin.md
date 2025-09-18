# intervaltypmodin

## Location
[src/backend/utils/adt/timestamp.c:1056-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1056-L1134)

## Overview
Validates and constructs type modifier values for the INTERVAL data type, processing precision and field range parameters to create a compressed typmod representation.

## Definition


## Detailed Description
The  function is responsible for processing the type modifier parameters for PostgreSQL's INTERVAL data type. It takes an array of integer type modifiers and validates them according to SQL standard interval specifications, then encodes them into a single 32-bit typmod value.

The function handles two key aspects of interval type specification:
1. **Range (field mask)**: A bitmap specifying which temporal fields (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) are permitted
2. **Precision**: Sub-second decimal precision specification

The typmod encoding stores the range in the high 16 bits and precision in the low 16 bits, allowing efficient representation of all SQL standard interval resolutions while supporting PostgreSQL's truncation-based implementation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : ArrayType pointer containing the type modifier array
  - : Integer array extracted from the ArrayType
  - : Number of elements in the type modifier array

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md)
  - INTERVAL_MASK
  - INTERVAL_FULL_RANGE
  - INTERVAL_FULL_PRECISION
  - INTERVAL_TYPMOD
  - MAX_INTERVAL_PRECISION
  - ereport/errcode/errmsg (error reporting)
- Called from (representative examples):
  - SQL type system during INTERVAL type declaration
  - Parser during interval type specification processing

## Notes and Other Information
- Validates specific combinations of interval fields according to SQL standard (e.g., YEAR TO MONTH, DAY TO SECOND)
- Supports precision values from 0 to MAX_INTERVAL_PRECISION, with automatic clamping and warnings
- Returns -1 for full range intervals without explicit precision
- Part of PostgreSQL's type system infrastructure for interval data type management
- Handles error cases gracefully with descriptive error messages for invalid type modifiers