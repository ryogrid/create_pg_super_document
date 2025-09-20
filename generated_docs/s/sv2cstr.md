# sv2cstr

## Location
[src/pl/plperl/plperl.h:89-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.h#L89-L146)

## Overview
Converts a Perl SV (scalar value) to a C string in the current database encoding, returning a palloc'ed copy of the original string.

## Definition

```c
static inline char *
sv2cstr(SV *sv)
```
## Detailed Description
This function safely converts a Perl SV to a C-style null-terminated string in PostgreSQL's current database encoding. It handles various edge cases and encoding conversions:

1. **Safe SV handling**: Creates a copy of readonly SVs, typeglobs, and other problematic SV types to avoid Perl crashes
2. **UTF-8 processing**: Requests UTF-8 encoded strings from Perl when not in SQL_ASCII mode
3. **Encoding conversion**: Converts from UTF-8 to the database's encoding using utf_u2e()
4. **Memory management**: Uses Perl's reference counting and PostgreSQL's memory management

The function is specifically designed to handle Perl's quirks, such as SvPVutf8() croaking on certain object types like typeglobs and readonly objects (e.g., $^V).

## Parameters / Member Variables
- : The Perl SV (scalar value) to convert to a C string

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - isGV_with_GP (Perl macro to check for typeglobs)
  - SvREFCNT_inc_simple_void (Perl reference counting)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (PostgreSQL encoding function)
  - PG_SQL_ASCII (PostgreSQL encoding constant)
  - [utf_u2e](../u/utf_u2e.md) (PostgreSQL UTF-8 to database encoding converter)
- Called from (representative examples):
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md)
  - [plperl_create_sub](../p/plperl_create_sub.md)
  - [plperl_call_perl_func](../p/plperl_call_perl_func.md)
  - [plperl_trigger_handler](../p/plperl_trigger_handler.md)
  - [plperl_spi_prepare](../p/plperl_spi_prepare.md)

## Notes and Other Information
- Returns a palloc'ed string that must be freed by the caller
- Handles embedded null bytes properly by using Perl's string length
- Optimized for SQL_ASCII databases by bypassing UTF-8 conversion
- Critical for safe interaction between Perl and PostgreSQL data types
- Part of the PL/Perl procedural language implementation