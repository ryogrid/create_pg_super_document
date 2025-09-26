# find_arguments

## Location
src/port/snprintf.c: 746 - 963

## Overview
Validates and extracts variable arguments for printf-style format strings that use positional parameter specifications (%n$).

## Definition


## Detailed Description
This function analyzes printf-style format strings containing positional parameters (like %1, %2) and extracts the corresponding arguments from a va_list. It performs comprehensive validation to ensure all argument references use consistent positional notation and that argument types match their format specifiers. The function is part of PostgreSQL's portable snprintf implementation and ensures compatibility with C99 positional parameter standards.

The function parses the format string character by character, identifying conversion specifiers and their associated argument positions. It builds an array mapping each position to its expected argument type, then extracts arguments from the va_list in the correct order. This enables format strings to reference arguments out of order (e.g., "%2 %1").

## Parameters / Member Variables
- : The printf-style format string containing positional parameter specifications
- : Variable argument list (va_list) containing the actual arguments to be formatted
- : Output array that will be filled with argument values indexed by their positional numbers

## Dependencies
- Functions called/Symbols referenced:
  - PrintfArgValue (struct type)
  - PG_NL_ARGMAX (constant defining maximum number of positional arguments)
  - PrintfArgType (enum type)
  - ATYPE_* constants (ATYPE_INT, ATYPE_LONG, ATYPE_LONGLONG, ATYPE_DOUBLE, ATYPE_CHARPTR, ATYPE_NONE)
  - va_arg (standard C macro for extracting variable arguments)
  - strchr (standard C library function)
  - Max (PostgreSQL macro for maximum value)

- Called from (representative examples):
  - dopr (main printf formatting function)
  - flushbuffer (output buffer management function)

## Notes and Other Information
- Returns true if the format string is valid and arguments are successfully extracted, false otherwise
- Enforces C99 standard requirement that all argument references must be either positional (%n$) or non-positional, but not mixed
- Supports all standard printf conversion specifiers (d, i, o, u, x, X, c, s, p, e, E, f, g, G, m, %)
- Handles width and precision specifiers, including dynamic ones (*n$)
- Limited to PG_NL_ARGMAX positional arguments to prevent resource exhaustion
- Part of PostgreSQL's platform-independent printf implementation for systems lacking proper C99 support