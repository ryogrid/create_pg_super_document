# evaluateSleep

## Location
[src/bin/pgbench/pgbench.c:3383-3427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3383-L3427)

## Overview
Parses and evaluates sleep command arguments, converting time specifications with optional units to microseconds for pgbench timing operations.

## Definition
```c
static bool evaluateSleep(Variables *variables, int argc, char **argv, int *usecs)
```

## Detailed Description
This function processes the arguments to a \\sleep meta-command in pgbench scripts. It supports both literal numeric values and variable references (prefixed with `:`) for sleep durations. The function accepts optional time unit suffixes (\"ms\" for milliseconds, \"s\" for seconds) and defaults to seconds if no unit is specified. It performs validation on numeric values and variable resolution, returning the calculated sleep time in microseconds through an output parameter. The function includes comprehensive error checking for undefined variables and invalid numeric values.

## Parameters / Member Variables
- `variables`: Pointer to Variables structure containing the current pgbench variable context for variable resolution
- `argc`: Number of command arguments passed to the sleep command
- `argv`: Array of command arguments, where argv[1] is the sleep time and argv[2] is optional unit specifier
- `usecs`: Output parameter to store the calculated sleep duration in microseconds

## Dependencies
- Functions called/Symbols referenced:
  - [Variables](../V/Variables.md) (variable storage structure)
  - [getVariable](../g/getVariable.md) (resolve variable values by name)
  - atoi (convert string to integer)
  - isdigit (validate numeric characters)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - pg_log_error (error logging)
- Called from (representative examples):
  - [executeMetaCommand](executeMetaCommand.md)

## Notes and Other Information
- Returns true on successful parsing, false on error conditions
- [Variable](../V/Variable.md) references are indicated by a leading `:` character (e.g., \":varname\")
- Supported time units: \"ms\" (milliseconds), \"s\" (seconds), default is seconds
- Validates that variable values are numeric (non-zero or starting with digit)
- Conversion factors: seconds × 1,000,000 = microseconds, milliseconds × 1,000 = microseconds
- Part of pgbench's meta-command processing system for script execution control
- The function is static with internal linkage within pgbench.c