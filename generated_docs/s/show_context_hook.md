# show_context_hook

## Location
src/bin/psql/startup.c: 1164 - 1184

## Overview
A validation and assignment hook function for the show_context parameter in psql that controls the visibility of error context information.

## Definition
```c
static bool show_context_hook(const char *newval)
```

## Detailed Description
This function validates and applies the show_context configuration parameter in psql. It accepts string values ("never", "errors", "always") and converts them to corresponding PostgreSQL show context enumeration values. The function also applies the setting to the current database connection if one exists. This hook is part of psql's variable system and ensures that only valid values are accepted for the show_context parameter.

## Parameters / Member Variables
- `newval`: The string value to validate and assign (must be one of "never", "errors", or "always")

## Dependencies
- Functions called/Symbols referenced:
  - PQSHOW_CONTEXT_NEVER (enumeration constant)
  - PQSHOW_CONTEXT_ERRORS (enumeration constant) 
  - PQSHOW_CONTEXT_ALWAYS (enumeration constant)
  - PsqlVarEnumError (for error reporting)
  - [PQsetErrorContextVisibility](../P/PQsetErrorContextVisibility.md) (to apply setting to database connection)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (at src/bin/psql/startup.c:1261)

## Notes and Other Information
- This is a static function defined in src/bin/psql/startup.c
- The function returns true on successful validation/assignment, false on invalid input
- Uses case-insensitive string comparison (pg_strcasecmp) for value matching
- Automatically applies the setting to the active database connection via PQsetErrorContextVisibility
- Part of psql's configuration variable system that manages display behavior for error contexts
- The three valid values control when PostgreSQL error context is shown: never, only on errors, or always