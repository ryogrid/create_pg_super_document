# anycompatiblemultirange_out

## Location
src/backend/utils/adt/pseudotypes.c: 249 - 262

## Overview
A pseudotype output function that provides string representation for anycompatiblemultirange pseudotype values by delegating to the concrete multirange type's output function.

## Definition
```c
Datum anycompatiblemultirange_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper for the anycompatiblemultirange pseudotype, which represents any multirange type that is compatible with other types in a function call. It directly delegates the actual output formatting to the `multirange_out` function, which handles the conversion of multirange values to their string representation. This delegation pattern allows PostgreSQL's type system to work with generic multirange types while ensuring type compatibility and proper output formatting.

The anycompatiblemultirange pseudotype is part of the "compatible" family of pseudotypes that ensure all parameters in a polymorphic function call resolve to compatible types, providing stronger type checking than the basic "any" pseudotypes.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the multirange value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - multirange_out
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:249-262
- This function is automatically used by PostgreSQL when converting anycompatiblemultirange values to text
- Part of the pseudotype infrastructure that enables polymorphic function definitions with enhanced type compatibility checking
- The actual formatting logic is handled by the concrete multirange type's output function
- Provides stronger type safety than basic anymultirange by ensuring type compatibility across function parameters