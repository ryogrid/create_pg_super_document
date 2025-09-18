# SvREFCNT_dec_current

## Location
src/pl/plperl/plperl.c: 312 - 322

## Overview
A static inline function that decrements the reference count of a given Perl SV (scalar value) within the currently active Perl interpreter, providing a convenient wrapper that handles interpreter context setup.

## Definition


## Detailed Description
This function serves as a convenience wrapper around Perl's SvREFCNT_dec macro. Its primary purpose is to simplify reference count management by automatically reloading the active Perl interpreter pointer using the dTHX macro, which saves notation in calling code that frequently switches between different Perl interpreter contexts. The function is designed to be inlined for performance efficiency while providing cleaner code structure in the PL/Perl implementation.

## Parameters / Member Variables
- : Pointer to the Perl SV (scalar value) whose reference count should be decremented

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl macro for setting up interpreter context)
  - SvREFCNT_dec (Perl macro for decrementing reference count)
- Called from (representative examples):
  - plperl_inline_handler
  - plperl_func_handler
  - plperl_trigger_handler
  - plperl_event_trigger_handler
  - free_plperl_function

## Notes and Other Information
- This function is particularly useful in PL/Perl where code frequently needs to manage Perl scalar reference counts
- The dTHX macro ensures the correct interpreter context is available for the SvREFCNT_dec operation
- Located in src/pl/plperl/plperl.c at lines 312-322
- The inline qualifier suggests this function is performance-critical and called frequently