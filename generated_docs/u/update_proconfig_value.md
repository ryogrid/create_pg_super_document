# update_proconfig_value

## Location
[src/backend/commands/functioncmds.c:645-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L645-L669)

## Overview
Updates a proconfig value according to a list of VariableSetStmt items, handling GUC parameter configuration for PostgreSQL functions.

## Definition

```c
static ArrayType *
update_proconfig_value(ArrayType *a, List *set_items)
```
## Detailed Description
This static function processes a list of SET/RESET statements to update function configuration parameters (proconfig). It iterates through a list of VariableSetStmt items and applies each configuration change to the provided ArrayType structure. The function handles both setting new values and resetting existing ones, including the special case of resetting all configuration parameters at once.

The function is used internally by PostgreSQL's function management system to maintain the proconfig array that stores function-specific configuration settings like search_path, work_mem, and other GUC parameters.

## Parameters / Member Variables
- : ArrayType pointer representing the current proconfig array (can be NULL for no existing config)
- : List of VariableSetStmt structures containing the configuration changes to apply

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_node (macro for list traversal)
  - [VariableSetStmt](../V/VariableSetStmt.md) (structure type)
  - VAR_RESET_ALL (enum constant)
  - [ExtractSetVariableArgs](../E/ExtractSetVariableArgs.md) (extracts value string from SET statement)
  - [GUCArrayAdd](../G/GUCArrayAdd.md) (adds a GUC parameter to array)
  - [GUCArrayDelete](../G/GUCArrayDelete.md) (removes a GUC parameter from array)
- Called from (representative examples):
  - [compute_function_attributes](../c/compute_function_attributes.md) (src/backend/commands/functioncmds.c:817)
  - [AlterFunction](../A/AlterFunction.md) (src/backend/commands/functioncmds.c:1483)

## Notes and Other Information
- The function handles the special VAR_RESET_ALL case by setting the entire array to NULL, effectively clearing all configuration
- Input and result may be NULL to signify a null proconfig entry
- Uses PostgreSQL's GUC (Grand Unified Configuration) system for parameter management
- Part of the function DDL (Data Definition Language) implementation in PostgreSQL