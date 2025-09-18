# anycompatiblerange_out

## Location
[src/backend/utils/adt/pseudotypes.c:223-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L223-L235)

## Overview
A pseudotype output function that provides string representation for anycompatiblerange pseudotype values by delegating to the concrete range type's output function.

## Definition


## Detailed Description
This function serves as a wrapper for the anycompatiblerange pseudotype, which represents any range type that is compatible with other types in a function call. It directly delegates the actual output formatting to the  function, which handles the conversion of range values to their string representation. This delegation pattern allows PostgreSQL's type system to work with generic range types while maintaining type safety and proper output formatting.

The function is part of PostgreSQL's pseudotype system that enables polymorphic functions to work with multiple compatible types while ensuring type consistency within a single function call.

## Parameters / Member Variables
- : Function call information structure containing the range value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - [range_out](../r/range_out.md)
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:223-235
- This function is automatically used by PostgreSQL when converting anycompatiblerange values to text
- Part of the pseudotype infrastructure that enables polymorphic function definitions
- The actual formatting logic is handled by the concrete range type's output function