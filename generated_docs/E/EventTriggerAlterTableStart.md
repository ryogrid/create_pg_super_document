# EventTriggerAlterTableStart

## Location
[src/backend/commands/event_trigger.c:1626-1659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1626-L1659)

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
  - [CollectedCommand](../C/CollectedCommand.md) (struct type)
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

## Simplified Source

```c
void
EventTriggerAlterTableStart(Node *parsetree)
{
    MemoryContext oldcxt;
    CollectedCommand *command;

    // Check if event trigger context is active
    if (!currentEventTriggerState ||
        currentEventTriggerState->commandCollectionInhibited)
        return;

    // Switch to event trigger memory context
    oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

    // Create new CollectedCommand for ALTER TABLE
    command = palloc(sizeof(CollectedCommand));
    command->type = SCT_AlterTable;
    command->in_extension = creating_extension;

    // Initialize ALTER TABLE specific fields
    command->d.alterTable.classId = RelationRelationId;
    command->d.alterTable.objectId = InvalidOid;  // Set later by EventTriggerAlterTableRelid
    command->d.alterTable.subcmds = NIL;

    // Copy parse tree and set up parent-child relationship
    command->parsetree = copyObject(parsetree);
    command->parent = currentEventTriggerState->currentCommand;
    currentEventTriggerState->currentCommand = command;

    MemoryContextSwitchTo(oldcxt);
}
```