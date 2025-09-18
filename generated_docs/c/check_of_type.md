# check_of_type

## Location
src/backend/commands/tablecmds.c: 6945 - 6987

## Overview
Validates that a type is suitable for use with CREATE TABLE OF or ALTER TABLE OF by ensuring it's a composite type created with CREATE TYPE AS.

## Definition
```c
void check_of_type(HeapTuple typetuple)
```

## Detailed Description
This function validates whether a given type can be used in typed table operations (CREATE TABLE OF and ALTER TABLE OF). It performs strict validation to ensure the type is a composite type that was originally created using CREATE TYPE AS statement. The function explicitly rejects other row types to avoid complex corner cases in DDL command handling and potential issues with domain constraints over composite types.

The validation process involves checking that the type is classified as TYPTYPE_COMPOSITE and that its underlying relation has the relkind RELKIND_COMPOSITE_TYPE. The function maintains an AccessShareLock on the type's relation until transaction commit to prevent concurrent modifications during typed table operations.

If the type fails validation, the function raises an error with an appropriate message indicating that the type is not a composite type.

## Parameters / Member Variables
- `typetuple`: A HeapTuple containing the pg_type row for the type being validated

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type
  - relation_open
  - relation_close
  - ereport
  - format_type_be
  - TYPTYPE_COMPOSITE
  - RELKIND_COMPOSITE_TYPE
  - OidIsValid
  - Assert
- Called from (representative examples):
  - ATExecAddOf
  - transformOfType

## Notes and Other Information
- The function deliberately restricts typed tables to types created with CREATE TYPE AS to simplify DDL command implementation
- Domain-over-composite types are explicitly not supported to avoid complex constraint handling scenarios
- The AccessShareLock is retained until transaction commit to prevent race conditions
- The function is part of PostgreSQL's typed table feature implementation
- Error messages use format_type_be() to provide user-friendly type names