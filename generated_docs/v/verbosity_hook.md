# verbosity_hook

## Location
src/bin/psql/startup.c: 1127 - 1149

## Overview
A variable hook function that processes changes to the VERBOSITY variable in psql, controlling how much detail is shown in PostgreSQL error messages.

## Definition


## Detailed Description
This hook function is responsible for parsing and setting the error message verbosity level when the VERBOSITY variable is modified in psql. It accepts one of four valid string values ("default", "verbose", "terse", "sqlstate") and maps them to corresponding PostgreSQL error verbosity constants. The function also applies the new verbosity setting to the current database connection if one exists. If an invalid value is provided, it displays an error message and returns false to indicate failure.

## Parameters / Member Variables
- : The new verbosity level as a string. Must be one of: "default", "verbose", "terse", or "sqlstate". Cannot be NULL (guaranteed by verbosity_substitute_hook).

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - PQERRORS_DEFAULT (PostgreSQL error verbosity constant)
  - PQERRORS_VERBOSE (PostgreSQL error verbosity constant)
  - PQERRORS_TERSE (PostgreSQL error verbosity constant)
  - PQERRORS_SQLSTATE (PostgreSQL error verbosity constant)
  - PsqlVarEnumError (error reporting function)
  - [PQsetErrorVerbosity](../P/PQsetErrorVerbosity.md) (PostgreSQL libpq function to set error verbosity)
- Called from (representative examples):
  - SetVariableHooks registration in EstablishVariableSpace

## Notes and Other Information
- Works in conjunction with verbosity_substitute_hook which ensures newval is never NULL
- The four verbosity levels control different amounts of detail in error messages:
  - "default": Standard error message format
  - "verbose": Includes additional context and details
  - "terse": Minimal error information
  - "sqlstate": Shows only SQL state codes
- Returns false only when an invalid verbosity level is specified
- Immediately applies the new setting to the current database connection if available
- Located in src/bin/psql/startup.c:1127