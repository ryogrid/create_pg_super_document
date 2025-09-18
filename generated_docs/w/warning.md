# warning

## Location
src/timezone/zic.c: 515 - 526

## Overview
A warning message function used in the timezone compiler (zic) to display warning messages to stderr and track warning state.

## Definition


## Detailed Description
This is a variadic function used throughout PostgreSQL's timezone compiler (zic) to issue warning messages. The function formats and displays warning messages to standard error, prefixed with "warning: ", and sets a global flag to indicate that warnings have been issued during compilation. It follows the standard warning pattern of accepting a format string and variable arguments, similar to printf-style functions.

The function uses the  function internally to handle the actual formatting and output of the message, while managing the variadic argument list. It also sets the global  variable to  to track that warnings have occurred, which can be used by the calling program to determine exit status or take other actions.

## Parameters / Member Variables
- : A format string (like printf) that specifies the warning message format
- : Variable arguments corresponding to the format specifiers in the string

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (stdarg.h macro)
  -  (internal function for error formatting)
  -  (stdarg.h macro)
  -  (global boolean variable)
- Called from (representative examples):
  -  (multiple locations in zic.c)
  -  (in zic.c)
  - Usage: /usr/bin/namecheck name (in zic.c)
  -  (in zic.c)
  -  (in zic.c)
  -  (in zic.c)
  -  (in zic.c)
  -  (in zic.c)
  - Various other functions in the timezone compilation process

## Notes and Other Information
- This function is specific to the timezone compiler ()
- Uses internationalization with the  macro for the "warning: " prefix
- The function is declared as , limiting its scope to the zic.c file
- Part of the timezone data compilation infrastructure, not the main PostgreSQL server
- The  global variable allows the program to track whether any warnings were issued
- Uses standard C variadic argument handling (, , )