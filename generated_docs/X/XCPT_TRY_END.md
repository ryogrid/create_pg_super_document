# XCPT_TRY_END

## Location
src/pl/plperl/ppport.h: 16930 - 16930

## Overview
A macro that marks the end of an exception handling try block in PL/Perl, serving as part of a custom exception handling mechanism for PostgreSQL's Perl procedural language.

## Definition


## Detailed Description
XCPT_TRY_END is a preprocessor macro defined in the ppport.h compatibility header for PL/Perl. It serves as the closing delimiter for exception handling try blocks when the NO_XSLOCKS compilation flag is defined and the dJMPENV macro is available. The macro expands to JMPENV_POP, which removes the current jump environment from the PostgreSQL exception handling stack.

This macro is part of a complete exception handling framework that includes:
- dXCPT: Declaration of exception handling variables
- XCPT_TRY_START: Beginning of a try block
- XCPT_TRY_END: End of a try block (this macro)
- XCPT_CATCH: Exception catch block
- XCPT_RETHROW: Re-throwing an exception

The macro is conditionally compiled and only defined when both NO_XSLOCKS and dJMPENV are defined, indicating it's used in environments where PostgreSQL's standard exception handling mechanisms are not available or need to be bypassed.

## Parameters / Member Variables
This macro takes no parameters and simply expands to the JMPENV_POP statement.

## Dependencies
- Functions called/Symbols referenced:
  - JMPENV_POP (PostgreSQL's jump environment pop function)
- Called from (representative examples):
  - Used in PL/Perl code blocks that need exception handling
  - Part of the XCPT exception handling macro family

## Notes and Other Information
- This macro is only defined when NO_XSLOCKS is defined, indicating special compilation conditions
- It's part of the ppport.h compatibility layer, which provides backward compatibility for Perl XS modules
- The macro must be used in conjunction with XCPT_TRY_START to properly bracket exception-handling code
- When dJMPENV is not available, an alternative implementation using Sigjmp_buf is provided
- Located in src/pl/plperl/ppport.h at line 16924
- This is part of PostgreSQL's PL/Perl procedural language implementation