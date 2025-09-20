# CStringGetDatum

## Location
[src/include/postgres.h:350-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L350-L359)

## Overview
CStringGetDatum converts a C string (null-terminated string) to PostgreSQL's internal Datum representation for use in the type system and function interfaces.

## Definition

```c
static inline Datum
CStringGetDatum(const char *X)
```
## Detailed Description
CStringGetDatum is a utility macro/function that converts a C-style null-terminated string into PostgreSQL's internal Datum format. It serves as a wrapper around PointerGetDatum(), treating the string as a pointer value. This function is essential for PostgreSQL's type system, particularly for output functions that need to return string representations of data.

The function is implemented as a static inline function for performance efficiency. Note that C strings are not full-fledged PostgreSQL types but are used extensively in type input/output operations and system catalogs.

## Parameters / Member Variables
- : A pointer to a null-terminated C string that will be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [enum_in](../e/enum_in.md) (enum input function)
  - [current_user](../c/current_user.md) (system information function)
  - [pg_get_viewdef_worker](../p/pg_get_viewdef_worker.md) (rule utilities)
  - [InputFunctionCall](../I/InputFunctionCall.md) (function manager)
  - [CreateRole](CreateRole.md) (user management)
  - many type input/output functions

## Notes and Other Information
- C strings are pass-by-reference in PostgreSQL's type system
- The caller must ensure the pointed-to string has adequate lifetime for the duration of its use
- This is primarily used by type output functions and system catalog operations
- The string content is not copied - only the pointer is stored in the Datum
- Care must be taken to ensure the string memory remains valid while the Datum is in use