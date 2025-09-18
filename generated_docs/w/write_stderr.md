# write_stderr

## Location
[src/bin/pg_ctl/pg_ctl.c:115-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L115-L201)

## Overview
A low-level error output function that writes formatted error messages to stderr or equivalent output mechanisms, designed for use during early PostgreSQL startup before the full error handling system is available.

## Definition


## Detailed Description
The  function provides a basic error output mechanism that can be used safely before PostgreSQL's full error reporting system (ereport/elog) is initialized. It handles platform-specific differences between Unix and Windows systems for error output.

On Unix systems, it directly uses  to write to stderr. On Windows, it determines whether PostgreSQL is running as a service or console application - if running as a service, it writes to the Windows event log using , otherwise it writes to the console using .

The function supports variable arguments like printf-style formatting and automatically applies internationalization translation to the format string using the  macro.

## Parameters / Member Variables
- : Format string (printf-style) that will be internationalized via 
- : Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  -  (Unix path)
  -  (Windows path)
  -  (Windows service detection)
  -  (Windows service logging)
  -  (Windows console output)
- Called from (representative examples):
  -  (bootstrap.c:282, 291)
  -  (postmaster.c:661, 734, 746, etc.)
  -  (assert.c:37, 40)
  -  (pg_ctl.c:254, 257, 270, etc.)
  -  (guc.c:1802, 1806, 1830, etc.)

## Notes and Other Information
- Used extensively throughout PostgreSQL for early startup error reporting
- Critical for debugging issues that occur before the main error handling system is available
- Automatically handles platform differences between Unix and Windows
- Format strings are automatically translated for internationalization
- Essential for pg_ctl utility error reporting
- Buffer size on Windows is arbitrarily set to 2048 characters