# get_perl_array_ref

## Location
src/pl/plperl/plperl.c: 1138 - 1169

## Overview
Extracts and returns a Perl array reference from a Scalar Value (SV), handling both regular Perl array references and PostgreSQL::InServer::ARRAY objects.

## Definition
static SV *get_perl_array_ref(SV *sv)

## Detailed Description
This function is responsible for safely extracting array references from Perl scalar values in the PL/Perl environment. It handles two specific cases:

1. **Regular Perl Array References**: If the input SV is a reference to a Perl array (SVt_PVAV), it returns the reference directly.

2. **PostgreSQL::InServer::ARRAY Objects**: If the input is a PostgreSQL::InServer::ARRAY object, it extracts the internal array field which contains the actual array reference.

The function performs validation to ensure the input is a valid reference and that any extracted array reference is properly formed. If a PostgreSQL::InServer::ARRAY object does not contain a valid array reference, it throws an error.

## Parameters / Member Variables
- sv: Input Scalar Value that may contain an array reference or be a PostgreSQL::InServer::ARRAY object

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - hv_fetch_string (fetches values from Perl hashes by string key)
- Called from (representative examples):
  - array_to_datum_internal
  - plperl_sv_to_datum
  - plperl_func_handler

## Notes and Other Information
- Returns NULL if the input SV is not a valid array reference or PostgreSQL::InServer::ARRAY object
- Uses Perl SvOK, SvROK, SvTYPE, and SvRV macros for type checking and reference validation
- The special handling of PostgreSQL::InServer::ARRAY objects allows for seamless integration between Perl arrays and PostgreSQL internal array representation
- Throws an ERROR (via elog) if a PostgreSQL::InServer::ARRAY object is malformed