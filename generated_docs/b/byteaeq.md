# byteaeq

## Location
[src/backend/utils/adt/varlena.c:3794-3825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3794-L3825)

## Overview
A PostgreSQL function that performs equality comparison between two bytea (binary string) values, returning true if they are identical.

## Definition

```c
Datum
byteaeq(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the equality operator for bytea data type in PostgreSQL. It compares two binary string values byte-by-byte to determine if they are equal. The function includes an optimization where it first compares the lengths of the two values using  to quickly determine inequality without detoasting the values if the lengths differ. If the lengths are equal, it detoasts both values and performs a byte-by-byte comparison using .

The function is designed to be memory-efficient for btree indexes by carefully managing memory and freeing working copies of toasted datums to prevent memory leaks.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides:
  - : First bytea value (as Datum)
  - : Second bytea value (as Datum)

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the size of a potentially toasted datum
  - : Converts Datum to bytea pointer with detoasting
  - : Macro to get variable-length data portion
  - : Standard C library function for memory comparison
  - : Macro to free memory if value was copied during detoasting
  - : Macro to return boolean result

- Called from (representative examples):
  - Used as the equality operator function for bytea type in SQL operations
  - Referenced by the PostgreSQL type system for bytea comparisons

## Notes and Other Information
- Optimized for performance by checking lengths first before detoasting
- Memory-safe implementation that prevents memory leaks in btree index operations
- Part of the bytea comparison function family in varlena.c
- Uses PostgreSQL's internal memory management macros for proper cleanup
- Located in src/backend/utils/adt/varlena.c:3794-3825