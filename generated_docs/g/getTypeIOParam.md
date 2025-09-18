# getTypeIOParam

## Location
src/backend/utils/cache/lsyscache.c: 2303 - 2324

## Overview
Determines the appropriate type OID parameter to pass to PostgreSQL I/O functions, implementing the logic for distinguishing between array types (which use their element type) and regular types (which use their own OID).

## Definition
```c
Oid getTypeIOParam(HeapTuple typeTuple)
```

## Detailed Description
The `getTypeIOParam` function encapsulates the logic for determining which type OID should be passed as the second parameter to PostgreSQL's type I/O functions (input/output functions). This function implements a key rule in PostgreSQL's type system: array types receive their element type OID (`typelem`) as the I/O parameter, while all other types receive their own type OID.

This distinction is crucial because array I/O functions are designed to work with the underlying element type rather than the array type itself. The function centralizes this knowledge to prevent incorrect direct usage of `typelem` elsewhere in the codebase, which would be wrong for non-array contexts.

Note that as of PostgreSQL 8.1, output functions only receive the value itself without auxiliary parameters, making this function primarily relevant for input functions, though the name remains for historical reasons.

## Parameters / Member Variables
- `typeTuple`: A HeapTuple representing a row from the `pg_type` system catalog containing the type information

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT (macro to extract struct from heap tuple)
  - OidIsValid (macro to check if OID is valid)
  - Form_pg_type (type catalog structure)
- Called from (representative examples):
  - stringTypeDatum (string type datum parsing)
  - get_type_io_data (type I/O information retrieval)
  - get_typdefault (type default value retrieval)
  - getTypeInputInfo (type input function information)
  - getTypeBinaryInputInfo (binary input function information)
  - compile_plperl_function (PL/Perl function compilation)
  - compile_pltcl_function (PL/Tcl function compilation)
  - plsample_func_handler (sample procedural language handler)

## Notes and Other Information
- For array types (where `typelem` is valid), returns the element type OID
- For non-array types, returns the type's own OID
- Critical for correct I/O function parameter passing in PostgreSQL's type system
- Centralizes the logic to avoid incorrect direct `typelem` usage elsewhere
- Primarily relevant for input functions since PostgreSQL 8.1 (output functions simplified)
- Used extensively in type I/O setup and procedural language compilation
- Essential for proper functioning of array type I/O operations
- Direct references to `typelem` for I/O purposes elsewhere in code are generally incorrect