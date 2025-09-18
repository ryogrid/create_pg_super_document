# croak_sv

## Location
src/pl/plperl/ppport.h: 14700 - 14712

## Overview
A macro that provides a standardized way to throw fatal errors in Perl extensions, handling both scalar values and reference-based error objects.

## Definition


## Detailed Description
The  macro is part of the Perl portability layer (ppport.h) that provides a consistent interface for throwing fatal errors across different Perl versions. It intelligently handles two types of error scenarios:

1. **Reference-based errors**: When the input SV is a reference (SvROK returns true), it sets the global error variable ERRSV to the referenced value and calls croak with NULL, allowing Perl's exception mechanism to use the custom error object.

2. **Scalar value errors**: When the input SV is not a reference, it applies UTF-8 fixes if necessary and formats the scalar value as a string using Perl's SVf format specifier before passing it to croak.

The macro uses STMT_START/STMT_END wrappers to ensure it can be used safely in all syntactic contexts, including as a single statement in if/else blocks.

## Parameters / Member Variables
- : A Perl scalar value (SV*) that contains either the error message as a string or a reference to an error object

## Dependencies
- Functions called/Symbols referenced:
  - SvROK (checks if SV is a reference)
  - sv_setsv (copies SV content)
  - ERRSV (global error variable)
  - croak (Perl's fatal error function)
  - D_PPP_FIX_UTF8_ERRSV_FOR_SV (UTF-8 handling macro)
  - SVf/SVfARG (string formatting macros)
  - STMT_START/STMT_END (statement block macros)

- Called from (representative examples):
  - [croak_cstr](croak_cstr.md) (in src/pl/plperl/plperl.h)
  - D_PPP_CROAK_IF_ERROR (in src/pl/plperl/ppport.h)
  - Various error handling contexts in PL/Perl

## Notes and Other Information
- This macro is defined in ppport.h (Perl Portability Port header) starting at line 14700
- Part of the Devel::PPPort compatibility layer that helps maintain compatibility across different Perl versions
- The macro handles UTF-8 encoding issues that can occur when passing string data between C and Perl
- Essential for proper error propagation in Perl XS extensions and embedded Perl code
- The dual-path approach (reference vs. scalar) allows for both simple string errors and complex error objects with additional metadata