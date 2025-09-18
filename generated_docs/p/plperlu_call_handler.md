# plperlu_call_handler

## Location
src/pl/plperl/plperl.c: 2067 - 2074

## Overview
Entry point function for PL/PerlU (untrusted Perl) language handler that delegates to the main Perl call handler.

## Definition
```c
Datum plperlu_call_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the specific entry point for the PL/PerlU (untrusted Perl) procedural language. It is a thin wrapper that immediately delegates to the main `plperl_call_handler` function, which handles the actual execution logic for both trusted (PL/Perl) and untrusted (PL/PerlU) Perl functions. The distinction between trusted and untrusted Perl is handled at a higher level in the system, allowing this function to simply pass through to the common implementation.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing arguments, context, and metadata for the function being called

## Dependencies
- Functions called/Symbols referenced:
  - [plperl_call_handler](plperl_call_handler.md)
  - PG_FUNCTION_INFO_V1
- Called from (representative examples):
  - [plperl_validator](plperl_validator.md)

## Notes and Other Information
- This is the handler function specifically registered for PL/PerlU (untrusted Perl) language
- The actual differentiation between trusted and untrusted Perl happens at the language registration level, not within this function
- Located in src/pl/plperl/plperl.c:2067-2074
- Forms part of PostgreSQL's procedural language interface for Perl