# CreateTrigger

## Location
src/backend/commands/trigger.c: 159 - 175

## Overview
CreateTrigger is a PostgreSQL function that creates a new trigger on a specified relation, serving as a wrapper around CreateTriggerFiringOn with default firing behavior.

## Definition


## Detailed Description
CreateTrigger is a simplified interface for creating triggers that delegates to CreateTriggerFiringOn with the default TRIGGER_FIRES_ON_ORIGIN firing behavior. This function handles the creation of database triggers with support for constraint triggers, partitioned tables, and various internal trigger scenarios. It provides extensive parameter flexibility to support different trigger creation contexts including user-initiated CREATE TRIGGER commands and internally generated triggers for constraints and foreign keys.

## Parameters / Member Variables
- : CreateTrigStmt structure containing the parsed CREATE TRIGGER statement
- : Source text of the CREATE TRIGGER command (required if whenClause is specified)
- : OID of the relation on which to create the trigger (0 to look up by name)
- : OID of the constraint reference relation (0 to look up by name)
- : OID of the constraint this trigger implements (0 for user triggers)
- : OID of associated constraint index (stored in pg_trigger.tgconstrindid)
- : OID of the trigger function (0 to use stmt->funcname)
- : OID of parent trigger for inheritance relationships
- : Pre-transformed WHEN expression (overrides stmt->whenClause)
- : Whether this is an internally-generated trigger
- : Indicates recursive call for partition triggers

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTriggerFiringOn](CreateTriggerFiringOn.md)
  - TRIGGER_FIRES_ON_ORIGIN
  - CreateTrigStmt
- Called from (representative examples):
  - [index_constraint_create](../i/index_constraint_create.md)
  - [CreateFKCheckTrigger](CreateFKCheckTrigger.md)
  - [createForeignKeyActionTriggers](../c/createForeignKeyActionTriggers.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- This function is essentially a wrapper that provides backward compatibility and simplified interface
- Automatically handles recursion to partitioned tables when isInternal is false
- Requires ACL_TRIGGER permissions on the relation and ACL_EXECUTE on trigger function for non-internal triggers
- Internal triggers bypass permission checks but caller must handle them appropriately
- Returns ObjectAddress of the created trigger for dependency tracking