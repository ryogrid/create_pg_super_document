# plperlu_validator

## Location
src/pl/plperl/plperl.c: 2083 - 2094

## Overview
Entry point function for validating PL/PerlU (untrusted Perl) function definitions, delegating to the main Perl validator.

## Definition
```c
Datum plperlu_validator(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the specific entry point for validating PL/PerlU (untrusted Perl) function definitions during CREATE FUNCTION operations. It is a thin wrapper that delegates to the main `plperl_validator` function, ensuring that the function call information (fcinfo) contains the correct language OID for PL/PerlU. The validator performs syntax checking, argument/return type validation, and optionally compiles the function body if check_function_bodies is enabled. The distinction between trusted and untrusted Perl validation is handled by passing the appropriate language OID through the fcinfo structure.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the function OID to validate and the validator's context

## Dependencies
- Functions called/Symbols referenced:
  - [plperl_validator](plperl_validator.md)
- Called from (representative examples):
  - [plperlu_inline_handler](plperlu_inline_handler.md)

## Notes and Other Information
- This is the validator function specifically registered for PL/PerlU (untrusted Perl) language
- Called during CREATE FUNCTION to validate syntax and semantics of PL/PerlU functions
- The comment indicates it passes 'our fcinfo so it gets our oid', ensuring the correct language OID is used for validation
- The actual validation logic is shared between PL/Perl and PL/PerlU in the main validator function
- Located in src/pl/plperl/plperl.c:2083-2094
- Always returns void as validator results are ignored by the system