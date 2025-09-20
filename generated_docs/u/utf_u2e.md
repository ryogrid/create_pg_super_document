# utf_u2e

## Location
[src/pl/tcl/pltcl.c:79-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L79-L84)

## Overview
A utility function in PL/Tcl that converts UTF-8 encoded strings to the database's current encoding.

## Definition

```c
static inline char *
utf_u2e(const char *src)
```
## Detailed Description
The  function is a convenience wrapper around PostgreSQL's  function, specifically designed for converting UTF-8 encoded strings to the database server's current encoding. This function is part of the PL/Tcl procedural language implementation and is used extensively when data needs to flow from Tcl (which uses UTF-8) to PostgreSQL's internal representation.

The function is implemented as a static inline function for performance efficiency, as it's called frequently during PL/Tcl operations. It performs character set conversion by calling  with the source string, its length, and the UTF-8 encoding identifier.

Note that this function may allocate memory via palloc during conversion, which could lead to memory leaks if used repeatedly without proper memory context management. For scenarios where this is a concern, wrapper macros are available.

## Parameters / Member Variables
- : A null-terminated UTF-8 encoded string to be converted to the database encoding

## Dependencies
- Functions called/Symbols referenced:
  - [pg_any_to_server](../p/pg_any_to_server.md)
  - PG_UTF8
- Called from (representative examples):
  - UTF_U2E (macro wrapper)
  - [pltcl_func_handler](../p/pltcl_func_handler.md)
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md)
  - throw_tcl_error
  - compile_pltcl_function
  - pltcl_returnnext
  - pltcl_build_tuple_result

## Notes and Other Information
- This function is part of a pair with  for bidirectional UTF-8/database encoding conversion
- The function may leak palloc'd memory when doing conversions, so it should be used judiciously
- Wrapper macros are available for scenarios where memory management is critical
- Located in src/pl/tcl/pltcl.c:79-84
- Used extensively throughout PL/Tcl for encoding conversions when interfacing with PostgreSQL internals