# AfterTriggersData

## Location
src/backend/commands/trigger.c: 3903 - 3918

## Overview
AfterTriggersData is the top-level structure that manages all deferred trigger state for a database session, including event storage, constraint states, and hierarchical query/transaction contexts.

## Definition
```c
typedef struct AfterTriggersData
{
    CommandId   firing_counter; /* next firing ID to assign */
    SetConstraintState state;   /* the active S C state */
    AfterTriggerEventList events;   /* deferred-event list */
    MemoryContext event_cxt;    /* memory context for events, if any */

    /* per-query-level data: */
    AfterTriggersQueryData *query_stack;   /* array of structs shown below */
    int         query_depth;    /* current index in above array */
    int         maxquerydepth;  /* allocated len of above array */

    /* per-subtransaction-level data: */
    AfterTriggersTransData *trans_stack;   /* array of structs shown below */
    int         maxtransdepth;  /* allocated len of above array */
} AfterTriggersData;
```

## Detailed Description
AfterTriggersData serves as the central control structure for PostgreSQL's deferred trigger execution system. It manages a hierarchical state system with transaction-level and query-level contexts, tracks constraint states, and maintains the global event queue. The structure implements a stack-based approach for handling nested queries and subtransactions, allowing proper isolation and rollback of trigger events. The firing_counter ensures each trigger firing gets a unique identifier for ordering purposes.

## Parameters / Member Variables
- `firing_counter`: Monotonically increasing counter for assigning unique IDs to trigger firings
- `state`: Current SET CONSTRAINTS state controlling which triggers are deferred vs immediate
- `events`: Global list of all deferred trigger events awaiting execution
- `event_cxt`: Dedicated memory context for trigger event allocation and cleanup
- `query_stack`: Dynamic array storing per-query trigger state for nested query handling
- `query_depth`: Current nesting level index into the query_stack array
- `maxquerydepth`: Allocated size of the query_stack array for bounds checking
- `trans_stack`: Dynamic array storing per-subtransaction state for rollback support
- `maxtransdepth`: Allocated size of the trans_stack array for bounds checking

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (for firing_counter)
  - SetConstraintState (for constraint state management)
  - [AfterTriggerEventList](AfterTriggerEventList.md) (for events storage)
  - [AfterTriggersQueryData](AfterTriggersQueryData.md) (for query stack entries)
  - [AfterTriggersTransData](AfterTriggersTransData.md) (for transaction stack entries)
- Called from (representative examples):
  - [AfterTriggersTableData](AfterTriggersTableData.md) (context field)

## Notes and Other Information
This structure represents the global state for the entire deferred trigger system and is typically stored in static memory or as part of the session state. The dual-stack design (query and transaction) allows PostgreSQL to properly handle complex scenarios involving nested queries, stored procedures, and subtransactions while maintaining trigger event isolation and rollback capabilities. The memory context management ensures efficient cleanup of trigger events when transactions complete or abort.