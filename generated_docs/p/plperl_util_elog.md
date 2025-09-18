# plperl_util_elog

## Location
src/pl/plperl/plperl.c: 4054 - 4092

## Overview
Implements PostgreSQL's elog() function for PL/Perl, providing error and message logging capabilities while properly handling Perl exceptions and PostgreSQL's error handling mechanism.

## Definition
void plperl_util_elog(int level, SV *msg)

## Detailed Description
This function serves as the bridge between Perl's error handling and PostgreSQL's elog system. It converts Perl scalar values to C strings and emits them through PostgreSQL's logging system. The function is designed to handle different error levels gracefully - for levels below ERROR, it simply logs the message and returns. When the level is ERROR, it catches PostgreSQL's longjmp and converts it into a Perl croak(), maintaining proper error propagation in the Perl environment.

The function uses PostgreSQL's PG_TRY/PG_CATCH exception handling mechanism to ensure memory context integrity and proper error state management. It intentionally omits SPI usage checks as logging is considered safe in most contexts.

## Parameters / Member Variables
- : The PostgreSQL error/log level (DEBUG, LOG, WARNING, ERROR, etc.)
- : A Perl scalar value (SV*) containing the message to be logged

## Dependencies
- Functions called/Symbols referenced:
  - [sv2cstr](../s/sv2cstr.md) (converts Perl SV to C string)
  - elog (PostgreSQL's logging function)
  - [pfree](pfree.md) (PostgreSQL memory management)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling macros)
  - [CopyErrorData](../C/CopyErrorData.md) (copies error information)
  - [FlushErrorState](../F/FlushErrorState.md) (resets error state)
  - [croak_cstr](../c/croak_cstr.md) (throws Perl exception)
- Called from (representative examples):
  - PL_PERL_H (header file declaration)

## Notes and Other Information
- The function is implemented out-of-line to avoid conflicts between XSUB.h and PostgreSQL's PG_TRY macros
- Memory context switching ensures proper cleanup even when errors occur
- The volatile qualifier on cmsg ensures proper handling during exception unwinding
- Error data is copied before being passed to Perl to maintain proper memory management
- The function assumes elog() cannot have internal failures severe enough to require transaction abort