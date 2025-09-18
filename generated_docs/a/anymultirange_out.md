# anymultirange_out

## Location
[src/backend/utils/adt/pseudotypes.c:236-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L236-L248)

## Overview
A pseudotype output function that provides string representation for anymultirange pseudotype values by delegating to the concrete multirange type's output function.

## Definition
```c
Datum anymultirange_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper for the anymultirange pseudotype, which represents any multirange type (a collection of ranges). It directly delegates the actual output formatting to the `multirange_out` function, which handles the conversion of multirange values to their string representation. This delegation pattern allows PostgreSQL's type system to work with generic multirange types while maintaining type safety and proper output formatting.

Multiranges are collections of non-overlapping, non-adjacent ranges of the same type. The anymultirange pseudotype enables polymorphic functions to work with any multirange type while ensuring type consistency.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the multirange value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - [multirange_out](../m/multirange_out.md)
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:236-248
- This function is automatically used by PostgreSQL when converting anymultirange values to text
- Part of the pseudotype infrastructure that enables polymorphic function definitions
- The actual formatting logic is handled by the concrete multirange type's output function
- Multiranges were introduced to support collections of ranges efficiently