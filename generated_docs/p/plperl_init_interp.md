# plperl_init_interp

## Location
src/pl/plperl/plperl.c: 705 - 753

## Overview
Creates and initializes a new Perl interpreter instance for PL/Perl execution, handling platform-specific locale issues and setting up the basic Perl environment.

## Definition


## Detailed Description
The  function creates a new Perl interpreter instance and performs the initial setup required for PL/Perl execution. This function handles the complex process of initializing Perl's interpreter system while working within PostgreSQL's environment constraints.

Key functionality includes:
- Creating embedding arguments for Perl interpreter startup
- Handling platform-specific locale preservation (especially on Windows)
- Managing Perl system initialization with proper error handling
- Setting up the require/dofile opcodes for security
- Running the initial Perl bootstrap code (PLC_PERLBOOT)
- Executing any user-defined on_init code
- Restoring locale settings after Perl initialization

The function is designed to create interpreters that can later be specialized as either trusted or untrusted through subsequent initialization calls.

## Parameters / Member Variables
None - the function takes no parameters and returns a new PerlInterpreter pointer.

## Dependencies
- Functions called/Symbols referenced:
  - setlocale (multiple calls for locale preservation)
  - [pstrdup](pstrdup.md)
  - perl_alloc
  - perl_construct
  - perl_parse
  - perl_run
  - PERL_SET_CONTEXT
  - PERL_SYS_INIT3 (conditional)
  - [pqsignal](pqsignal.md)
  - [FloatExceptionHandler](../F/FloatExceptionHandler.md)
  - elog
  - ereport
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md)
  - [plperl_init_shared_libs](plperl_init_shared_libs.md)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md)
  - [select_perl_context](../s/select_perl_context.md)

## Notes and Other Information
- Handles Windows-specific locale preservation to prevent Perl from corrupting PostgreSQL's locale settings
- Uses conditional compilation for PERL_SYS_INIT3 based on platform and malloc implementation
- Restores SIGFPE handler after Perl initialization due to Perl's unfriendly signal handling
- Sets up the require opcode infrastructure that will be customized by trusted/untrusted initialization
- Supports optional on_init code execution for user customization
- Critical for security - prepares the foundation for later trust-level-specific hardening
- Returns fully constructed but not yet specialized (trusted/untrusted) Perl interpreter
- Error handling includes context information for debugging Perl initialization failures