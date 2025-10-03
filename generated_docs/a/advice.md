# advice

## Location
[src/bin/pg_config/pg_config.c:110-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_config/pg_config.c#L110-L115)

## Overview
A simple utility function that displays a brief message directing users to use the --help option for more detailed information.

## Definition
```c
static void advice(void)
```

## Detailed Description
The advice function is a minimal helper that provides guidance to users when they encounter command-line usage errors or need assistance. It outputs a standardized message using the program name stored in the global progname variable, directing users to consult the help system for comprehensive usage information. This function is typically called when invalid arguments are provided or when the program cannot proceed due to user input issues.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard C library function)
  - stderr (standard error stream)
  - _() (internationalization macro)
  - progname (global variable containing program name)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_config/pg_config.c:183)

## Notes and Other Information
- Uses internationalization support for localized error messages
- Outputs to stderr rather than stdout, following standard practice for error/advisory messages
- Part of the pg_config utility's error handling mechanism
- Simple but essential for user experience when command-line errors occur
- Follows the common Unix convention of suggesting --help for assistance