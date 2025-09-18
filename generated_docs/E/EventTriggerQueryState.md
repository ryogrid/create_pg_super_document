# EventTriggerQueryState

## Location
src/backend/commands/event_trigger.c: 59 - 79

## Overview
EventTriggerQueryState is a struct that maintains state information for event trigger processing during command execution, managing memory context, SQL drop operations, table rewrites, and command collection.

## Definition
```c
typedef struct EventTriggerQueryState
{
    /* memory context for this state's objects */
    MemoryContext cxt;

    /* sql_drop */
    slist_head  SQLDropList;
    bool        in_sql_drop;

    /* table_rewrite */
    Oid         table_rewrite_oid;    /* InvalidOid, or set for table_rewrite event */
    int         table_rewrite_reason; /* AT_REWRITE reason */

    /* Support for command collection */
    bool        commandCollectionInhibited;
    CollectedCommand *currentCommand;
    List       *commandList;          /* list of CollectedCommand; see deparse_utility.h */
    struct EventTriggerQueryState *previous;
} EventTriggerQueryState;
```

## Detailed Description
EventTriggerQueryState serves as a comprehensive state management structure for PostgreSQL's event trigger system. It maintains context and state information needed during the execution of DDL commands that may trigger event triggers. The structure supports nested command execution through its linked-list design using the `previous` pointer, allowing for proper state management during recursive operations.

The structure is divided into several functional areas: memory management, SQL drop operation tracking, table rewrite event handling, and command collection for event trigger processing. This design enables PostgreSQL to properly track and respond to various DDL events that event triggers are designed to intercept.

## Parameters / Member Variables
- `cxt`: Memory context used for allocating objects related to this event trigger state
- `SQLDropList`: Singly-linked list head for tracking SQL DROP operations
- `in_sql_drop`: Boolean flag indicating whether currently processing a SQL DROP operation
- `table_rewrite_oid`: Object ID of the table being rewritten, or InvalidOid if not in a table rewrite event
- `table_rewrite_reason`: The specific ALTER TABLE reason code for table rewrite operations
- `commandCollectionInhibited`: Boolean flag to disable command collection when needed
- `currentCommand`: Pointer to the currently processed CollectedCommand structure
- `commandList`: List of CollectedCommand structures for event trigger processing
- `previous`: Pointer to the previous EventTriggerQueryState, enabling nested state management

## Dependencies
- Functions called/Symbols referenced:
  - slist_head (PostgreSQL singly-linked list header)
  - CollectedCommand (command collection structure from deparse_utility.h)
  - MemoryContext (PostgreSQL memory management)
  - List (PostgreSQL list type)
  - Oid (PostgreSQL object identifier type)

- Called from (representative examples):
  - EventTriggerBeginCompleteQuery (src/backend/commands/event_trigger.c:1186, 1200)
  - EventTriggerEndCompleteQuery (src/backend/commands/event_trigger.c:1230)

## Notes and Other Information
This structure is central to PostgreSQL's event trigger implementation, providing the necessary state tracking for proper event trigger execution. The nested design (via the `previous` pointer) allows for handling complex scenarios where commands may trigger other commands that also need event trigger processing. The structure is defined in src/backend/commands/event_trigger.c and is primarily used internally by the event trigger subsystem for maintaining execution context and state across DDL operations.