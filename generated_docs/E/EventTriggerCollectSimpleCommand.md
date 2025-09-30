# EventTriggerCollectSimpleCommand

## Location
[src/backend/commands/event_trigger.c:1588-1625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1588-L1625)

## Overview
Saves data about a simple DDL command that was just executed, collecting it for later processing by event triggers.

## Definition
```c
void EventTriggerCollectSimpleCommand(ObjectAddress address,
                                     ObjectAddress secondaryObject,
                                     Node *parsetree)
```

## Detailed Description
This function is a core component of PostgreSQL's event trigger DDL command collection system. It creates and stores a CollectedCommand structure containing information about a simple DDL command that was just executed. The collected commands are later made available to event trigger functions through pg_event_trigger_ddl_commands().

The function allocates memory in the event trigger context, creates a CollectedCommand of type SCT_Simple, and appends it to the command list. It includes safety checks to ensure collection only occurs when event triggers are active and not inhibited.

## Parameters / Member Variables
- `address`: ObjectAddress identifying the primary object being operated on
- `secondaryObject`: ObjectAddress of a related object (meaning is command-specific, e.g., old schema in ALTER obj SET SCHEMA)  
- `parsetree`: Parse tree node representing the executed command

## Dependencies
- Functions called/Symbols referenced:
  - [CollectedCommand](../C/CollectedCommand.md) (struct type)
  - SCT_Simple (command type constant)
  - copyObject (for deep copying the parse tree)
  - [palloc](../p/palloc.md), lappend (memory and list management)
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md) (src/backend/catalog/index.c:3647)
  - [CreateOpFamily](../C/CreateOpFamily.md) (src/backend/commands/opclasscmds.c:317)
  - [AlterPublicationOptions](../A/AlterPublicationOptions.md) (src/backend/commands/publicationcmds.c:1048)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (multiple locations in src/backend/tcop/utility.c)

## Notes and Other Information
- Part of the DDL command collection framework that supports event triggers
- Only collects commands when currentEventTriggerState is active and collection is not inhibited
- Uses the event trigger memory context to ensure proper memory management
- The secondaryObject parameter provides additional context specific to each command type (e.g., source schema for ALTER SET SCHEMA commands)
- Creates a deep copy of the parse tree to ensure data integrity across different execution contexts
- Collected commands become available to event trigger functions via pg_event_trigger_ddl_commands()

## Simplified Source

```c
void EventTriggerCollectSimpleCommand(ObjectAddress address,
                                     ObjectAddress secondaryObject,
                                     Node *parsetree)
{
    CollectedCommand *command;

    // Return early if event triggers not active or collection disabled
    if (!currentEventTriggerState ||
        currentEventTriggerState->commandCollectionInhibited)
        return;

    // Switch to event trigger memory context
    MemoryContext oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

    // Create and populate command structure
    command = palloc(sizeof(CollectedCommand));
    command->type = SCT_Simple;
    command->in_extension = creating_extension;
    command->d.simple.address = address;
    command->d.simple.secondaryObject = secondaryObject;
    command->parsetree = copyObject(parsetree);

    // Add to command list
    currentEventTriggerState->commandList =
        lappend(currentEventTriggerState->commandList, command);

    // Restore previous memory context
    MemoryContextSwitchTo(oldcxt);
}
```