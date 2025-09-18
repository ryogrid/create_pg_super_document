# pp_require_safe

## Location
src/pl/plperl/plperl.c: 880 - 917

## Overview
A secure implementation of Perl's require opcode that prevents loading of external code by only allowing access to already-loaded modules.

## Definition


## Detailed Description
The  function is a security-focused replacement for Perl's standard require opcode. It implements a "safe" require mechanism that completely prevents loading of new code from external files while still allowing access to modules that have already been loaded into the interpreter.

The function works by:
1. Extracting the module/file name from the Perl stack
2. Checking if the requested module exists in Perl's %INC hash (which tracks loaded modules)
3. Returning true if the module is already loaded
4. Dying with an error message if the module is not already loaded

This approach ensures that PL/Perl functions can use standard Perl syntax like "use Foo;" but only if module Foo was pre-loaded during interpreter initialization, thus preventing arbitrary code execution.

## Parameters / Member Variables
- : Perl threading context (standard Perl interpreter context parameter)

## Dependencies
- Functions called/Symbols referenced:
  - dVAR (Perl macro for variable declarations)
  - dSP (Perl macro for stack pointer)
  - POPs (Perl macro to pop value from stack)
  - SvPV (Perl macro to get string value and length)
  - hv_fetch (Perl function to fetch from hash)
  - GvHVn (Perl macro to get hash from glob)
  - PL_incgv (Perl global variable for %INC)
  - PL_sv_undef (Perl undefined scalar value)
  - RETPUSHNO/RETPUSHYES (Perl macros for return values)
  - DIE (Perl macro for throwing exceptions)
  - aTHX_ (Perl threading context for function calls)
- Called from (representative examples):
  - set_interp_require (assigns this function to opcodes)
  - plperl_trusted_init (sets up trusted interpreter security)

## Notes and Other Information
- Critical security component that prevents arbitrary code loading in PL/Perl
- Replaces both OP_REQUIRE and OP_DOFILE opcodes in trusted interpreters
- Allows standard Perl "use" and "require" syntax while maintaining security
- Only permits access to modules loaded during interpreter initialization
- Designed specifically for trusted PL/Perl execution environment
- Contains compiler-specific code to handle variations in Perl's DIE macro behavior
- Part of PL/Perl's defense-in-depth security architecture
- Returns Perl OP pointer (standard for Perl opcode implementations)