# timestamptz_scale

## Location
[src/backend/utils/adt/timestamp.c:879-899](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L879-L899)

## Overview
Adjusts a timestamptz value for a specified scale factor (precision), used by PostgreSQL's type system to format timestamp columns according to their declared precision.

## Definition

```c
struct pg_itm_in tt,
			   *itm_in = &tt;
```
## Detailed Description
The  function is a PostgreSQL type system function that adjusts the precision of a timestamptz (timestamp with time zone) value according to the specified typmod (type modifier). It takes a timestamptz value and a precision specification, then returns the timestamp adjusted to the appropriate number of fractional seconds. This function is automatically called by PostgreSQL when storing values in timestamptz columns that have explicit precision declarations (e.g.,  for millisecond precision).

## Parameters / Member Variables
-  (PG_GETARG_TIMESTAMPTZ(0)): The input timestamptz value to be scaled
-  (PG_GETARG_INT32(1)): The type modifier specifying the desired precision (number of fractional seconds)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ (macro for extracting timestamptz argument)
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md) (performs the actual precision adjustment)
  - PG_RETURN_TIMESTAMPTZ (macro for returning timestamptz result)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's type system infrastructure and is typically invoked automatically when values are stored in timestamptz columns with explicit precision
- The actual precision adjustment logic is delegated to 
- The function follows PostgreSQL's standard function calling convention using  and return macros