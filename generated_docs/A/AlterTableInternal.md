# AlterTableInternal

## Location
src/backend/commands/tablecmds.c: 4428 - 4472

## Overview
AlterTableInternal is an internal function that performs ALTER TABLE operations on a relation specified by its OID, handling the core logic without requiring parse transformation or utility command context.

## Definition


## Detailed Description
AlterTableInternal provides a streamlined entry point for ALTER TABLE operations when the target relation is already identified by its OID rather than by name. This function is designed for internal use cases where the relation may already be open by calling layers, making it unsafe for alterations that could break existing query plans. The function operates without an AlterTableUtilityContext, which means it cannot handle subcommand types that require parse transformation or could generate subcommands needing ProcessUtility.

The function follows a straightforward execution pattern: it determines the appropriate lock level based on the commands, opens the relation with that lock, triggers any relevant event triggers, and delegates the actual work to ATController.

## Parameters / Member Variables
- : The OID of the relation to be altered
- : A list of ALTER TABLE subcommands to execute
- : Boolean flag indicating whether to apply changes recursively to child tables

## Dependencies
- Functions called/Symbols referenced:
  - AlterTableGetLockLevel
  - relation_open
  - EventTriggerAlterTableRelid
  - ATController
- Called from (representative examples):
  - AlterTableMoveAll
  - DefineVirtualRelation

## Notes and Other Information
- This function does not reject operations on already-open relations, as it assumes callers may have the relation open
- Cannot be used for alterations that could break existing query plans
- Does not reject pending AFTER triggers
- Lacks AlterTableUtilityContext, limiting its use to simpler subcommand types
- Primarily used for internal operations where relation identification and basic validation have already been performed