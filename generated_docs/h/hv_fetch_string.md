# hv_fetch_string

## Location
src/pl/plperl/plperl.c: 4120 - 4142

## Overview
A utility function that retrieves a Perl scalar value (SV) from a Perl hash table using a string key, with proper encoding conversion from the database encoding to UTF-8.

## Definition
static SV **hv_fetch_string(HV *hv, const char *key)

## Detailed Description
This function serves as the counterpart to hv_store_string(), providing a convenient wrapper around Perl's hv_fetch() function specifically designed for PostgreSQL's PL/Perl implementation. It handles the same character encoding conversion complexity between PostgreSQL's database encoding and UTF-8 that is required for proper Perl hash key handling. The function converts the input key from the database encoding to UTF-8 using PostgreSQL's encoding conversion functions, then retrieves the value from the hash using Perl's native hv_fetch() function with UTF-8 encoding indicators.

Like its store counterpart, it uses a negative key length parameter to signal to Perl's hv_fetch() that the key is UTF-8 encoded, ensuring consistent Unicode handling in Perl hash operations.

## Parameters / Member Variables
- : Pointer to the Perl hash (HV) from which the value will be retrieved
- : C string key in the current database encoding to look up in the hash

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - [pg_server_to_any](../p/pg_server_to_any.md) (PostgreSQL encoding conversion function)
  - PG_UTF8 (PostgreSQL UTF-8 encoding constant)
  - hv_fetch (Perl hash retrieval function)
  - [pfree](../p/pfree.md) (PostgreSQL memory management)
- Called from (representative examples):
  - [get_perl_array_ref](../g/get_perl_array_ref.md) (for array reference retrieval)
  - [plperl_modify_tuple](../p/plperl_modify_tuple.md) (for tuple modification operations)
  - [plperl_spi_exec_prepared](../p/plperl_spi_exec_prepared.md) (for prepared statement execution)

## Notes and Other Information
- The function is static, indicating it's only used within the plperl.c file
- Memory management mirrors hv_store_string - converted keys are freed if they differ from the original
- UTF-8 encoding is indicated by using a negative key length in the hv_fetch() call
- Comments reference hv_store_string for implementation details, showing the functions are designed as a matched pair
- Essential for proper internationalization support when retrieving values from PL/Perl hash structures
- Returns a pointer to SV* (double pointer) following Perl's hash API conventions
- Returns NULL if the key is not found in the hash