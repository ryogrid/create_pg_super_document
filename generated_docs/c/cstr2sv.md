# cstr2sv

## Location
src/pl/plperl/plperl.h: 147 - 174

## Overview
Creates a new Perl SV (scalar value) from a C string assumed to be in the current database's encoding.

## Definition


## Detailed Description
This function converts a C-style null-terminated string from PostgreSQL's database encoding into a Perl SV. The conversion process depends on the current database encoding:

1. **SQL_ASCII handling**: For SQL_ASCII databases, creates the SV directly without encoding conversion
2. **UTF-8 conversion**: For other encodings, converts the string from database encoding to UTF-8 using utf_e2u()
3. **Perl SV creation**: Creates a new Perl SV and properly marks it as UTF-8 encoded when applicable
4. **Memory management**: Frees the intermediate UTF-8 string after SV creation

This function is the counterpart to sv2cstr(), enabling bidirectional conversion between PostgreSQL and Perl string representations.

## Parameters / Member Variables
- : A C string in the current database encoding to convert to a Perl SV

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (PostgreSQL encoding function)
  - PG_SQL_ASCII (PostgreSQL encoding constant)
  - [utf_e2u](../u/utf_e2u.md) (PostgreSQL database encoding to UTF-8 converter)
  - newSVpv (Perl function to create new SV from string)
  - SvUTF8_on (Perl macro to mark SV as UTF-8)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
- Called from (representative examples):
  - [make_array_ref](../m/make_array_ref.md)
  - [plperl_trigger_build_args](../p/plperl_trigger_build_args.md)
  - [plperl_create_sub](../p/plperl_create_sub.md)
  - [plperl_hash_from_tuple](../p/plperl_hash_from_tuple.md)
  - [plperl_spi_execute_fetch_result](../p/plperl_spi_execute_fetch_result.md)
  - [croak_cstr](croak_cstr.md)

## Notes and Other Information
- Returns a new Perl SV that follows Perl's memory management rules
- Optimized for SQL_ASCII databases by bypassing encoding conversion
- Properly handles UTF-8 marking for non-ASCII databases
- Essential for passing PostgreSQL data to Perl functions
- Part of the PL/Perl procedural language implementation
- The returned SV is managed by Perl's garbage collector