# parse_func_options

## Location
[src/backend/commands/foreigncmds.c:529-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L529-L568)

## Overview
Processes the function options (handler and validator) for CREATE and ALTER FOREIGN DATA WRAPPER commands, validating and extracting the function OIDs for the FDW handler and validator functions.

## Definition
```c
static void parse_func_options(ParseState *pstate, List *func_options,
                              bool *handler_given, Oid *fdwhandler,
                              bool *validator_given, Oid *fdwvalidator)
```

## Detailed Description
This function parses and validates the function-related options specified in CREATE or ALTER FOREIGN DATA WRAPPER statements. It processes a list of function options and extracts the handler and validator function specifications, looking up their corresponding OIDs. The function ensures that each option (handler/validator) is specified at most once and reports conflicts if duplicates are found. If an option is not specified, the corresponding output parameter is set to InvalidOid.

## Parameters / Member Variables
- `pstate`: ParseState context for error reporting and parsing operations
- `func_options`: List of DefElem structures containing the function options to process
- `handler_given`: Output parameter indicating whether a handler function was specified
- `fdwhandler`: Output parameter containing the OID of the handler function (InvalidOid if not given)
- `validator_given`: Output parameter indicating whether a validator function was specified
- `fdwvalidator`: Output parameter containing the OID of the validator function (InvalidOid if not given)

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md)
  - [lookup_fdw_handler_func](../l/lookup_fdw_handler_func.md)
  - [lookup_fdw_validator_func](../l/lookup_fdw_validator_func.md)
- Called from (representative examples):
  - [CreateForeignDataWrapper](../C/CreateForeignDataWrapper.md)
  - [AlterForeignDataWrapper](../A/AlterForeignDataWrapper.md)

## Notes and Other Information
- This is a static helper function used internally by FDW management commands
- The function initializes all output parameters to safe defaults (false for booleans, InvalidOid for OIDs)
- Only recognizes "handler" and "validator" options; any other option names result in an error
- Prevents duplicate specification of the same option type within a single statement
- Part of PostgreSQL's Foreign Data Wrapper infrastructure for managing external data sources