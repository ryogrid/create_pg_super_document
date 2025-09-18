# CreateProceduralLanguage

## Location
src/backend/commands/proclang.c: 37 - 225

## Overview
Creates a new procedural language or replaces an existing one in the PostgreSQL system, handling all aspects of language definition including handler functions, validation, and dependency management.

## Definition


## Detailed Description
This function implements the CREATE LANGUAGE SQL command functionality. It validates the language definition, creates or updates the pg_language catalog entry, and establishes proper dependencies. The function performs comprehensive validation of handler functions, manages ownership and permissions, and ensures proper catalog consistency.

Key operations include:
- Superuser privilege verification
- Handler function validation and type checking
- Optional inline and validator function validation
- Catalog entry creation or update with proper locking
- Dependency record management for proper cleanup
- Extension membership recording
- Post-creation hook invocation

The function supports both creating new languages and replacing existing ones when the REPLACE option is specified.

## Parameters / Member Variables
- : Pointer to CreatePLangStmt containing the parsed CREATE LANGUAGE statement with all language definition details including name, handler function, trust level, and optional inline/validator functions

## Dependencies
- Functions called/Symbols referenced:
  - superuser
  - LookupFuncName
  - get_func_rettype
  - NameListToString
  - table_open
  - SearchSysCache1
  - heap_modify_tuple
  - CatalogTupleUpdate
  - GetNewOidWithIndex
  - heap_form_tuple
  - CatalogTupleInsert
  - deleteDependencyRecordsFor
  - recordDependencyOnOwner
  - recordDependencyOnCurrentExtension
  - record_object_address_dependencies
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- Requires superuser privileges to create custom procedural languages
- Handler function must return language_handler type
- Inline function (if specified) must accept internal type parameter
- Validator function (if specified) must accept oid type parameter
- When replacing existing language, preserves OID, ownership, and ACL permissions
- Creates dependencies on handler, inline, and validator functions to ensure proper cleanup
- Automatically records extension membership if created within an extension context
- Function is located in src/backend/commands/proclang.c:37-225