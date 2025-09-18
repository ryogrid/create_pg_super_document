# anycompatiblearray_out

## Location
src/backend/utils/adt/pseudotypes.c: 178 - 183

## Overview
A wrapper function that provides text output capability for the anycompatiblearray pseudotype by delegating to the array_out function.

## Definition
Datum anycompatiblearray_out(PG_FUNCTION_ARGS)

## Detailed Description
The anycompatiblearray_out function serves as a text output function for the anycompatiblearray pseudotype in PostgreSQL. It acts as a thin wrapper around the array_out function, simply forwarding the function call information (fcinfo) to array_out to handle the actual text serialization. This design allows the anycompatiblearray pseudotype to leverage the existing array text output infrastructure without duplicating code. The anycompatiblearray pseudotype is part of PostgreSQL's enhanced polymorphic type system introduced to provide better type resolution for functions with multiple polymorphic parameters.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call information macro that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [array_out](array_out.md): The actual implementation for text array output
- Called from (representative examples):
  - No direct references found in the codebase (typically called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:178-183
- Part of PostgreSQL's enhanced pseudotype system for handling polymorphic types with compatibility constraints
- The anycompatiblearray pseudotype ensures that all anycompatible* parameters in a function call resolve to compatible types
- Text output functions convert internal data representations to human-readable string format
- This function works in conjunction with other anycompatible* pseudotypes to provide type safety in polymorphic functions