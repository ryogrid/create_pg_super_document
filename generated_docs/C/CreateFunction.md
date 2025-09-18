# CreateFunction

## Location
src/backend/commands/functioncmds.c: 1011 - 1292

## Overview
Executes a CREATE FUNCTION or CREATE PROCEDURE utility statement, orchestrating the complete process of function definition validation, parsing, and catalog registration.

## Definition
```c
ObjectAddress CreateFunction(ParseState *pstate, CreateFunctionStmt *stmt)
```

## Detailed Description
This function is the main entry point for processing CREATE FUNCTION and CREATE PROCEDURE statements. It performs comprehensive validation of all function attributes, handles language-specific processing, validates permissions and security constraints, processes parameter lists and return types, interprets the function body according to the target language, and finally creates the function in the system catalog.

The function handles both regular functions and procedures, with special logic for different programming languages (C, SQL, PL/pgSQL, etc.). It validates user privileges for the target namespace and language, processes transform types, handles default values for cost and row estimates, and coordinates with ProcedureCreate() for the actual catalog entry creation.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and source text tracking
- `stmt`: CreateFunctionStmt containing all parsed function definition elements

## Dependencies
- Functions called/Symbols referenced:
  - QualifiedNameGetCreationNamespace (resolves function name and namespace)
  - object_aclcheck, aclcheck_error (permission checking)
  - compute_function_attributes (processes function options and attributes)
  - SearchSysCache1, ReleaseSysCache (language catalog lookups)
  - extension_file_exists (checks for language extensions)
  - superuser (privilege validation)
  - typenameTypeId, get_base_element_type (type resolution)
  - get_transform_oid (transform function validation)
  - interpret_function_parameter_list (parameter processing)
  - compute_return_type (return type resolution)
  - construct_array_builtin (array construction for transforms)
  - interpret_AS_clause (function body processing)
  - ProcedureCreate (final catalog entry creation)
  - Various constants: PROVOLATILE_VOLATILE, PROPARALLEL_UNSAFE, PROKIND_*
- Called from (representative examples):
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1655)

## Notes and Other Information
- Handles both CREATE FUNCTION and CREATE PROCEDURE with shared logic and procedure-specific branches
- Validates language permissions: trusted languages require USAGE, untrusted languages require superuser
- Only superusers can create leakproof functions due to security implications
- Sets intelligent defaults for COST (1 for C/internal, 100 for others) and ROWS (1000 for set-returning, 0 otherwise)
- Supports transform types for custom type handling in procedural languages
- Validates that ROWS parameter is only specified for set-returning functions
- Coordinates with the parser state to provide accurate error locations and context
- Returns ObjectAddress for dependency tracking and object management
- Central orchestrator in PostgreSQL's function DDL implementation, calling multiple specialized helper functions