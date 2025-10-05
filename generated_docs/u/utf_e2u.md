# utf_e2u

## Location
[src/pl/tcl/pltcl.c:85-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L85-L89)

## Overview
A utility function in PL/Tcl that converts strings from the database's current encoding to UTF-8 encoding.

## Definition

```c
static inline char *
utf_e2u(const char *src)
```
## Detailed Description
The `utf_e2u` function is the counterpart to `utf_u2e`, providing conversion from PostgreSQL's database server encoding to UTF-8. This function is essential for the PL/Tcl procedural language implementation when data needs to flow from PostgreSQL's internal representation to Tcl, which expects UTF-8 encoded strings.

Like its counterpart, this function is implemented as a static inline function for performance efficiency and serves as a convenience wrapper around PostgreSQL's `pg_server_to_any` function. It automatically determines the source string length and specifies UTF-8 as the target encoding.

The function may allocate memory during the conversion process, potentially leading to memory leaks if used repeatedly without proper memory context management. Wrapper macros are available for scenarios where memory management is critical.

## Parameters / Member Variables
- `src`: A null-terminated string in the database's current encoding to be converted to UTF-8

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - PG_UTF8
- Called from (representative examples):
  - UTF_E2U (macro wrapper)
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md)
  - [pltcl_event_trigger_handler](../p/pltcl_event_trigger_handler.md)

## Notes and Other Information
- This function is the complement to `utf_u2e` for bidirectional UTF-8/database encoding conversion
- The function may leak palloc'd memory when doing conversions, so it should be used judiciously
- Wrapper macros are available for scenarios where memory management is critical
- Located in src/pl/tcl/pltcl.c:85-89
- Primarily used in trigger and event trigger handlers where database values need to be passed to Tcl
- Also used in PL/Perl for similar encoding conversion purposes

## Simplified Source

```c
static inline char *utf_e2u(const char *src) {
    // Convert database server encoding to UTF-8
    return pg_server_to_any(src, strlen(src), PG_UTF8);
}
```