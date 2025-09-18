# plperl_destroy_interp

## Location
src/pl/plperl/plperl.c: 918 - 956

## Overview
Performs controlled cleanup of a Perl interpreter by running END blocks while avoiding full destruction that could impact PostgreSQL's stability.

## Definition


## Detailed Description
The  function implements a minimal but safe cleanup strategy for Perl interpreters in the PL/Perl environment. Rather than performing a full perl_destruct() which could have unpredictable side effects on PostgreSQL's process state, this function takes a conservative approach by only running Perl END blocks.

The function's design philosophy prioritizes PostgreSQL's stability over complete Perl cleanup. It:
- Runs END blocks to allow Perl code to perform manual cleanup
- Avoids calling perl_destruct() due to potential adverse effects
- Uses Perl's exception handling mechanism (JMPENV) to safely execute END blocks
- Performs basic cleanup of temporaries and stack state
- Sets the interpreter pointer to NULL to prevent reuse

This approach allows Perl modules and user code to clean up resources through END blocks while maintaining the integrity of the PostgreSQL process.

## Parameters / Member Variables
- : Double pointer to PerlInterpreter instance; allows the function to nullify the pointer after cleanup

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl macro for threading context)
  - dJMPENV (Perl macro for exception handling setup)
  - JMPENV_PUSH/JMPENV_POP (Perl exception handling macros)
  - call_list (Perl function to execute array of subroutines)
  - LEAVE (Perl macro for scope cleanup)
  - FREETMPS (Perl macro for temporary cleanup)
  - PERL_UNUSED_VAR (Perl macro to suppress warnings)
  - PL_exit_flags, PL_endav, PL_minus_c, PL_scopestack_ix (Perl global variables)
- Called from (representative examples):
  - plperl_fini (during process cleanup)

## Notes and Other Information
- Implements minimal destruction strategy to avoid perl_destruct() side effects
- Uses exception handling to safely run END blocks without crashing PostgreSQL
- Conservative approach prioritizes PostgreSQL stability over complete cleanup
- Allows Perl code to perform manual resource cleanup through END blocks
- Part of PL/Perl's safe shutdown procedure during process termination
- Designed to be called only when the target interpreter is the active one
- Sets interpreter pointer to NULL to prevent accidental reuse after destruction
- Based on Perl's own perl_destruct() implementation but significantly simplified