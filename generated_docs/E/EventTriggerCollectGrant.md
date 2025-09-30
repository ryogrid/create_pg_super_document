# EventTriggerCollectGrant

## Location
[src/backend/commands/event_trigger.c:1751-1794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1751-L1794)

## Overview
Saves data about a GRANT/REVOKE command being executed for event trigger processing, creating a deep copy of the InternalGrant structure to ensure proper lifetime management.

## Definition

```c
void
EventTriggerCollectGrant(InternalGrant *istmt)
```
## Detailed Description
This function is responsible for collecting information about GRANT and REVOKE commands during their execution. It's part of PostgreSQL's event trigger system that allows users to define triggers responding to DDL operations.

The function performs a comprehensive deep copy of the InternalGrant structure because the original might not have the appropriate lifetime for event trigger processing. This includes copying not just the main structure but also all its list members (objects, grantees, and column privileges).

After creating the copy, the function constructs a CollectedCommand structure with type SCT_Grant and adds it to the current event trigger state's command list. This allows event triggers to access detailed information about grant operations that occurred during command execution.

## Parameters / Member Variables
- : Pointer to an InternalGrant structure containing the details of the GRANT/REVOKE command being executed, including objects, grantees, and privilege information

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory context management
  -  - Memory allocation in current context
  -  - Memory copying function
  -  - [List](../L/List.md) copying for objects and grantees
  -  - Deep copy for column privileges
  -  - [List](../L/List.md) append operation
  -  - [List](../L/List.md) cell access macro
  -  - Empty list constant
  -  - [Command](../C/Command.md) type constant
- Called from (representative examples):
  -  - GRANT/REVOKE statement execution

## Notes and Other Information
- Only operates when event trigger context is active and command collection is not inhibited
- Performs tedious but necessary deep copying to ensure all data has proper lifetime
- Uses the event trigger's memory context to ensure copied data persists beyond the current operation
- Sets the command's in_extension field based on the creating_extension global variable
- The parsetree field is set to NULL since InternalGrant structures don't correspond directly to parse trees
- Part of PostgreSQL's comprehensive event trigger system for DDL command monitoring
- Handles the complexity of copying nested list structures containing various privilege-related data

## Simplified Source

```c
void
EventTriggerCollectGrant(InternalGrant *istmt)
{
    MemoryContext oldcxt;
    CollectedCommand *command;
    InternalGrant *icopy;
    ListCell *cell;

    // Skip if event triggers not active or collection disabled
    if (!currentEventTriggerState ||
        currentEventTriggerState->commandCollectionInhibited)
        return;

    // Switch to event trigger memory context
    oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

    // Create deep copy of InternalGrant structure
    icopy = palloc(sizeof(InternalGrant));
    memcpy(icopy, istmt, sizeof(InternalGrant));

    // Copy object and grantee lists
    icopy->objects = list_copy(istmt->objects);
    icopy->grantees = list_copy(istmt->grantees);

    // Deep copy column privileges list
    icopy->col_privs = NIL;
    foreach(cell, istmt->col_privs)
        icopy->col_privs = lappend(icopy->col_privs, copyObject(lfirst(cell)));

    // Create command entry for event trigger system
    command = palloc(sizeof(CollectedCommand));
    command->type = SCT_Grant;
    command->in_extension = creating_extension;
    command->d.grant.istmt = icopy;
    command->parsetree = NULL;

    // Add to event trigger command list
    currentEventTriggerState->commandList =
        lappend(currentEventTriggerState->commandList, command);

    MemoryContextSwitchTo(oldcxt);
}
```