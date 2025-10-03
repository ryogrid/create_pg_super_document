# croak_cstr

## Location
[src/pl/plperl/plperl.h:175-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.h#L175-L206)

## Overview
Triggers a Perl croak (fatal error) with a specified message given in the database encoding, safely handling non-ASCII characters.

## Definition

```c
static inline void
croak_cstr(const char *str)
```
## Detailed Description
This function provides a safe way to trigger Perl fatal errors (croak) with error messages containing non-ASCII characters. It addresses limitations in Perl's standard croak() function which does not handle non-ASCII data properly.

The implementation uses two different approaches depending on Perl version:

1. **Modern Perl (with croak_sv)**: Converts the database-encoded string to a Perl SV using cstr2sv() and passes it to croak_sv()
2. **Older Perl (without croak_sv)**: Manually converts to UTF-8, creates an error SV with location information using mess(), sets the error variable (@), and calls croak(NULL)

The function ensures proper error location reporting and UTF-8 handling across different Perl versions.

## Parameters / Member Variables
- `*str`: A C string in the current database encoding containing the error message
## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - [croak_sv](croak_sv.md) (Modern Perl error function, conditionally compiled)
  - sv_2mortal (Perl function to mark SV for garbage collection)
  - [cstr2sv](cstr2sv.md) (Custom function to convert C string to Perl SV)
  - get_sv (Perl function to get special variables like @)
  - [utf_e2u](../u/utf_e2u.md) (PostgreSQL database encoding to UTF-8 converter)
  - mess (Perl function to create formatted error message with location)
  - SvUTF8_on (Perl macro to mark SV as UTF-8)
  - sv_setsv (Perl function to copy SV contents)
  - croak (Standard Perl error function)
- Called from (representative examples):
  - [plperl_spi_exec](../p/plperl_spi_exec.md)
  - [plperl_return_next](../p/plperl_return_next.md)
  - [plperl_spi_query](../p/plperl_spi_query.md)
  - [plperl_spi_prepare](../p/plperl_spi_prepare.md)
  - [plperl_util_elog](../p/plperl_util_elog.md)

## Notes and Other Information
- Never returns - always terminates execution with a Perl exception
- Handles UTF-8 encoding properly across different Perl versions
- Uses conditional compilation to support both old and new Perl APIs
- Essential for proper error reporting in PL/Perl functions
- Ensures error location information is preserved in older Perl versions
- Part of the PL/Perl procedural language implementation
- The function is designed to be a drop-in replacement for croak() when dealing with database-encoded strings