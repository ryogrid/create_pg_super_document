# pg_dependencies_in

## Location
src/backend/statistics/dependencies.c: 653 - 669

## Overview
This function serves as the input routine for the pg_dependencies data type but deliberately prevents text input by throwing an error, as this type is designed for internal binary storage only.

## Definition


## Detailed Description
The function is part of PostgreSQL's type system infrastructure for the pg_dependencies pseudo-type, which stores functional dependency statistics. However, it intentionally disallows text input parsing since pg_dependencies data is meant to be stored and manipulated only in binary form. The type exists primarily to support storage of dependency statistics in system catalogs, not for user input/output operations.

When called, the function immediately raises an error with ERRCODE_FEATURE_NOT_SUPPORTED, preventing any attempt to convert textual input into a pg_dependencies value. This design pattern is common for PostgreSQL internal types that should not accept user input.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro (standard PostgreSQL function calling convention)
- No specific parameters as input is not actually processed

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_VOID (PostgreSQL macro for returning void from a function)
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md) (error code assignment)
  - [errmsg](../e/errmsg.md) (error message formatting)

- Called from (representative examples):
  - Not directly called (registered as type input function in system catalogs)

## Notes and Other Information
- Part of the pg_dependencies type definition infrastructure
- Registered in the PostgreSQL type system but prevents actual usage for input
- Follows PostgreSQL convention for internal-only data types
- The error message specifically mentions the type name to aid debugging
- Returns void to satisfy compiler requirements despite throwing an error