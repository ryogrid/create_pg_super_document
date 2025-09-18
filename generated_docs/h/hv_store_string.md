# hv_store_string

## Location
src/pl/plperl/plperl.c: 4093 - 4119

## Overview
A utility function that stores a Perl scalar value (SV) into a Perl hash table using a string key, with proper encoding conversion from the database encoding to UTF-8.

## Definition
static SV **hv_store_string(HV *hv, const char *key, SV *val)

## Detailed Description
This function provides a convenient wrapper around Perl's hv_store() function specifically designed for PostgreSQL's PL/Perl implementation. It handles the complexity of character encoding conversion between PostgreSQL's database encoding and UTF-8, which is required for proper Perl hash key handling. The function converts the input key from the database encoding to UTF-8 using PostgreSQL's encoding conversion functions, then stores the value in the hash using Perl's native hv_store() function with UTF-8 encoding indicators.

The function uses a negative key length parameter to signal to Perl's hv_store() that the key is UTF-8 encoded, ensuring proper Unicode handling in Perl hash operations.

## Parameters / Member Variables
- : Pointer to the Perl hash (HV) where the value will be stored
- : C string key in the current database encoding
- : Perl scalar value (SV*) to be stored in the hash

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - pg_server_to_any (PostgreSQL encoding conversion function)
  - PG_UTF8 (PostgreSQL UTF-8 encoding constant)
  - hv_store (Perl hash storage function)
  - pfree (PostgreSQL memory management)
- Called from (representative examples):
  - plperl_trigger_build_args (multiple times for trigger argument construction)
  - plperl_event_trigger_build_args (for event trigger arguments)
  - plperl_create_sub (for subroutine creation)
  - plperl_hash_from_tuple (for tuple to hash conversion)
  - plperl_spi_execute_fetch_result (for SPI result processing)

## Notes and Other Information
- The function is static, indicating it's only used within the plperl.c file
- Memory management is handled carefully - converted keys are freed if they differ from the original
- UTF-8 encoding is indicated by using a negative key length in the hv_store() call
- Essential for proper internationalization support in PL/Perl hash operations
- Used extensively throughout PL/Perl for building hash structures from PostgreSQL data