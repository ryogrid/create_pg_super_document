# transformRelOptions

## Location
src/backend/access/common/reloptions.c: 1156 - 1339

## Overview
Transforms a list of relation option definitions (DefElem) into a text array format suitable for storage in pg_class.reloptions, handling CREATE/ALTER/RESET operations with namespace filtering.

## Definition


## Detailed Description
This function is the core transformation engine for PostgreSQL relation options. It processes a list of option definitions and converts them into the standardized text array format used internally by PostgreSQL. The function handles three main scenarios: CREATE TABLE/INDEX (building from scratch), ALTER TABLE SET (adding/modifying options), and ALTER TABLE RESET (removing options). It performs namespace validation, merges new options with existing ones, and formats each option as 'name=value' strings. The function also includes special handling for deprecated OIDS options and validates that option names don't contain '=' characters.

## Parameters / Member Variables
- : Existing reloptions as Datum (text array format), may be NULL
- : List of DefElem nodes containing new option definitions to process  
- : Target namespace to filter options (NULL means no namespace)
- : Array of valid namespace strings, NULL-terminated (NULL means only NULL namespace valid)
- : Whether to allow 'oids=false' for backwards compatibility
- : True for RESET operations, false for CREATE/SET operations

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - DatumGetArrayTypeP  
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - VARDATA/VARSIZE macros
  - [accumArrayResult](../a/accumArrayResult.md)
  - [makeArrayResult](../m/makeArrayResult.md)
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)
  - [DefElem](../D/DefElem.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (table creation)
  - [DefineIndex](../D/DefineIndex.md) (index creation)
  - [ATExecSetOptions](../A/ATExecSetOptions.md) (ALTER TABLE)
  - [CreateTableSpace](../C/CreateTableSpace.md) (tablespace creation)

## Notes and Other Information
- Returns text array as Datum, or (Datum) 0 if no options
- For RESET operations, validates that no values are specified (syntax checking)
- Handles special case for deprecated OIDS option with backwards compatibility
- Each option is stored as 'name=value' format, with 'name=true' assumed for bare names
- Namespace filtering allows different subsystems to manage their own option sets
- Function is defined in src/backend/access/common/reloptions.c:1156-1339