# anyrange_out

## Location
src/backend/utils/adt/pseudotypes.c: 210 - 222

## Overview
A wrapper function that provides text output capability for the anyrange pseudotype by delegating to the range_out function.

## Definition
Datum anyrange_out(PG_FUNCTION_ARGS)

## Detailed Description
The anyrange_out function serves as a text output function for the anyrange pseudotype in PostgreSQL. It acts as a thin wrapper around the range_out function, simply forwarding the function call information (fcinfo) to range_out to handle the actual text serialization of range values. This design allows the anyrange pseudotype to leverage the existing range text output infrastructure without duplicating code. The anyrange pseudotype allows functions to accept range types without specifying the particular range type, providing polymorphism for range types (such as int4range, tsrange, daterange, etc.) in PostgreSQL's type system.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call information macro that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - range_out: The actual implementation for text range output
  - PSEUDOTYPE_DUMMY_INPUT_FUNC: Referenced in the surrounding context
- Called from (representative examples):
  - No direct references found in the codebase (typically called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/pseudotypes.c:210-222
- Part of PostgreSQL's pseudotype system for handling polymorphic range types
- The anyrange pseudotype allows functions to accept any range type (int4range, numrange, tsrange, daterange, etc.)
- Range types represent intervals of values and are displayed in formats like [1,10) or (2023-01-01,2023-12-31]
- Text output functions convert internal range representations to human-readable string format for display
- This pseudotype enables writing generic functions that can work with different range types without knowing their specific element types at compile time
- Essential for polymorphic range operations and generic range-handling functions