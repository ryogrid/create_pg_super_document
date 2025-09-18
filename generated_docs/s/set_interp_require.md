# set_interp_require

## Location
src/pl/plperl/plperl.c: 490 - 508

## Overview
A static function that configures Perl's require and dofile operations to use either safe or original implementations based on whether the interpreter is running in trusted mode.

## Definition


## Detailed Description
This function modifies Perl's internal operation dispatch table (PL_ppaddr) to control how 'require' and 'dofile' operations are handled based on the trust level of the current Perl interpreter context. When running in trusted mode, it sets both OP_REQUIRE and OP_DOFILE to use the safe implementation (pp_require_safe) which restricts module loading for security. When running in untrusted mode, it restores the original Perl behavior (pp_require_orig) allowing normal module loading. This is a critical security mechanism in PL/Perl that prevents trusted Perl code from loading arbitrary modules that could compromise database security.

## Parameters / Member Variables
- : Boolean flag indicating whether to use safe (true) or original (false) require/dofile implementations

## Dependencies
- Functions called/Symbols referenced:
  - PL_ppaddr (Perl's operation dispatch table)
  - pp_require_safe (safe implementation of require/dofile operations)
  - pp_require_orig (original Perl require/dofile implementation)
  - OP_REQUIRE (Perl opcode constant for require operation)
  - OP_DOFILE (Perl opcode constant for dofile operation)
- Called from (representative examples):
  - select_perl_context
  - activate_interpreter

## Notes and Other Information
- Critical security function that enforces trusted vs untrusted Perl execution contexts
- The safe implementation (pp_require_safe) allows access only to modules that have already been loaded
- This mechanism prevents trusted PL/Perl functions from loading potentially dangerous external modules
- Part of PL/Perl's comprehensive security model for running untrusted code safely
- Located in src/pl/plperl/plperl.c at lines 490-508
- The function directly manipulates Perl's internal dispatch table for maximum security effectiveness