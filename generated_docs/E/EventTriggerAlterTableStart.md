# EventTriggerAlterTableStart

## Location
src/backend/commands/event_trigger.c: 1626 - 1659

## Overview
Prepares to receive data on an ALTER TABLE command about to be executed by setting up a CollectedCommand structure as the current command.

## Definition
```c
void EventTriggerAlterTableStart(Node *parsetree)
```

## Detailed Description
This function initiates the collection process for ALTER TABLE commands, which are more complex than simple DDL commands because they can contain multiple subcommands. Instead of immediately adding the command to the command list like EventTriggerCollectSimpleCommand does, it creates a CollectedCommand of type SCT_AlterTable and sets it as the current command in the event trigger state.

The function establishes a parent-child relationship by linking the new command to any existing currentCommand, enabling nested command tracking. The actual collection is completed later when all subcommands have been processed.

## Parameters / Member Variables
- `parsetree`: Parse tree node representing the ALTER TABLE command being started

## Dependencies
- Functions called/Symbols referenced:
  - CollectedCommand (struct type)
  - SCT_AlterTable (command type constant)  
  - copyObject (for deep copying the parse tree)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [AlterTableMoveAll](../A/AlterTableMoveAll.md) (src/backend/commands/tablecmds.c:15537)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (multiple locations in src/backend/tcop/utility.c)
  - [ProcessUtilityForAlterTable](../P/ProcessUtilityForAlterTable.md) (src/backend/tcop/utility.c:1985)

## Notes and Other Information
- Part of the ALTER TABLE command collection framework for event triggers
- Works in conjunction with EventTriggerAlterTableRelid to complete ALTER TABLE command collection
- Uses a deferred collection approach - the command is not added to commandList until all subcommands are processed
- Supports nested ALTER TABLE commands through the parent-child command relationship
- Initializes the subcmds list as NIL, which gets populated as subcommands are processed
- Sets objectId to InvalidOid initially - the actual relation OID is set later via EventTriggerAlterTableRelid
- Only operates when event trigger context is active and collection is not inhibited