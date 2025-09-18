# ValidIOData

## Location
src/backend/utils/adt/misc.c: 54 - 62

## Overview
ValidIOData is a structure used to cache metadata needed for input validation operations in PostgreSQL's type conversion functions.

## Definition


## Detailed Description
ValidIOData serves as a cache structure to optimize repeated input validation operations by storing pre-computed type information. This structure is primarily used in the `pg_input_is_valid_common` function to avoid redundant lookups of type input functions and their parameters across multiple calls. The structure maintains the necessary metadata for converting string input to the target PostgreSQL data type, including the type's OID, type modifier, input function information, and whether the type name is constant across calls.

## Parameters / Member Variables
- `typoid`: The OID (Object Identifier) of the PostgreSQL data type being validated
- `typmod`: The type modifier that provides additional type-specific information (e.g., precision for numeric types)
- `typname_constant`: Boolean flag indicating whether the type name argument remains constant across function calls, enabling optimization
- `typiofunc`: The OID of the input function used to convert string representations to the target type
- `typioparam`: Additional parameter passed to the type input function
- `inputproc`: Function manager information structure containing the compiled input function for efficient repeated calls

## Dependencies
- Functions called/Symbols referenced:
  - Used as a type definition only (no direct function calls)
- Called from (representative examples):
  - pg_input_is_valid_common

## Notes and Other Information
This structure is designed for performance optimization in input validation scenarios where the same type validation is performed repeatedly. By caching the type metadata, PostgreSQL avoids expensive type system lookups on subsequent calls. The structure is typically allocated in the function's memory context (`fn_mcxt`) and stored in `fn_extra` for persistence across multiple function invocations. The `typname_constant` field enables further optimization when the type name doesn't change between calls.