# EventTriggerCollectCreateOpClass

## Location
[src/backend/commands/event_trigger.c:1828-1861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1828-L1861)

## Overview
Saves data about a CREATE OPERATOR CLASS command being executed for event trigger processing, collecting information about new operator class definitions.

## Definition

```c
void
EventTriggerCollectCreateOpClass(CreateOpClassStmt *stmt, Oid opcoid,
								 List *operators, List *procedures)
```
## Detailed Description
This function is part of PostgreSQL's event trigger system and is responsible for collecting information about CREATE OPERATOR CLASS commands during their execution. It captures comprehensive details about the creation of new operator classes, which are essential components of PostgreSQL's indexing infrastructure.

The function creates a CollectedCommand structure with type SCT_CreateOpClass and stores the operator class OID, lists of associated operators and procedures, and a copy of the original statement. This information enables event triggers to monitor and respond to the creation of new operator classes.

Operator classes define how PostgreSQL's indexing methods (like B-tree, GiST, GIN, etc.) work with specific data types. This function ensures that event triggers have access to all the details about newly created operator classes, including their constituent operators and support procedures.

## Parameters / Member Variables
- `*stmt`: Pointer to a CreateOpClassStmt structure representing the parsed CREATE OPERATOR CLASS command
- `opcoid`: OID of the newly created operator class
- `*operators`: List of operators that are part of this operator class definition
- `*procedures`: List of support procedures that are part of this operator class definition
## Dependencies
- Functions called/Symbols referenced:
  -  - Memory context switching
  -  - Zero-initialized memory allocation in current context
  -  - Object address initialization macro
  -  - Deep copy of parse tree nodes
  -  - [List](../L/List.md) append operation
  -  - [Command](../C/Command.md) type constant
  -  - [Relation](../R/Relation.md) OID constant
- Called from (representative examples):
  -  - Main operator class definition function

## Notes and Other Information
- Only operates when event trigger context is active and command collection is not inhibited
- Uses palloc0() instead of palloc() to ensure the command structure is zero-initialized
- Uses the event trigger's memory context to ensure collected data persists beyond the current operation
- Sets the command's in_extension field based on the creating_extension global variable
- Creates a proper ObjectAddress for the operator class using ObjectAddressSet macro
- Stores both the operators and procedures lists directly since they are assumed to have appropriate lifetime
- Part of PostgreSQL's operator class infrastructure that enables custom indexing behavior
- Critical for event triggers that need to monitor the creation of indexing infrastructure components

## Simplified Source

```c
void
EventTriggerCollectCreateOpClass(CreateOpClassStmt *stmt, Oid opcoid,
                                 List *operators, List *procedures)
{
    MemoryContext oldcxt;
    CollectedCommand *command;

    // Check if event trigger collection is active
    if (!currentEventTriggerState ||
        currentEventTriggerState->commandCollectionInhibited)
        return;

    // Switch to event trigger memory context
    oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

    // Create command structure
    command = palloc0(sizeof(CollectedCommand));
    command->type = SCT_CreateOpClass;
    command->in_extension = creating_extension;

    // Set operator class address
    ObjectAddressSet(command->d.createopc.address,
                     OperatorClassRelationId, opcoid);

    // Store operators and procedures lists
    command->d.createopc.operators = operators;
    command->d.createopc.procedures = procedures;

    // Copy the original statement
    command->parsetree = (Node *) copyObject(stmt);

    // Add to event trigger command list
    currentEventTriggerState->commandList =
        lappend(currentEventTriggerState->commandList, command);

    MemoryContextSwitchTo(oldcxt);
}
```