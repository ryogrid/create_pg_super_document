# AfterTriggerSharedData

## Location
[src/backend/commands/trigger.c:3718-3726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3718-L3726)

## Overview
AfterTriggerSharedData is a structure that stores common information shared among multiple after-trigger events to optimize memory usage in PostgreSQL's trigger execution system.

## Definition
```c
typedef struct AfterTriggerSharedData
{
    TriggerEvent ats_event;         /* event type indicator, see trigger.h */
    Oid          ats_tgoid;         /* the trigger's ID */
    Oid          ats_relid;         /* the relation it's on */
    CommandId    ats_firing_id;     /* ID for firing cycle */
    struct AfterTriggersTableData *ats_table;    /* transition table access */
    Bitmapset   *ats_modifiedcols;  /* modified columns */
} AfterTriggerSharedData;
```

## Detailed Description
This structure contains data that can be shared among multiple after-trigger events that have similar characteristics. By sharing common information like trigger ID, relation ID, and event type among multiple events, PostgreSQL reduces memory consumption when processing large numbers of similar trigger events. The structure is part of the after-trigger optimization system that groups events into chunks and minimizes redundant data storage.

## Parameters / Member Variables
- `ats_event`: TriggerEvent enumeration indicating the type of triggering event (INSERT, UPDATE, DELETE, etc.)
- `ats_tgoid`: Object ID (Oid) of the specific trigger that will be executed
- `ats_relid`: Object ID (Oid) of the relation (table) on which the trigger is defined
- `ats_firing_id`: CommandId that identifies the firing cycle for this group of trigger events
- `ats_table`: Pointer to AfterTriggersTableData structure for accessing transition tables (OLD/NEW table references)
- `ats_modifiedcols`: Bitmapset indicating which columns were modified (relevant for UPDATE triggers with column-specific firing conditions)

## Dependencies
- Functions called/Symbols referenced:
  - TriggerEvent (enumeration from trigger.h)
  - CommandId (PostgreSQL command identifier type)
  - AfterTriggersTableData (transition table structure)
  - Bitmapset (PostgreSQL bitmap utility type)
- Called from (representative examples):
  - AfterTriggerShared (typedef pointer)
  - afterTriggerAddEvent
  - AfterTriggerSaveEvent

## Notes and Other Information
- Central to PostgreSQL's memory optimization strategy for after-trigger processing
- Designed to be shared among multiple AfterTriggerEvent records that have identical trigger execution context
- The ats_firing_id helps coordinate trigger execution order and grouping
- Transition table support (ats_table) enables triggers to access OLD and NEW pseudo-tables
- Column modification tracking (ats_modifiedcols) supports efficient UPDATE trigger filtering
- Part of the chunk-based trigger event storage system that minimizes memory fragmentation
- Located in src/backend/commands/trigger.c within the after-trigger execution framework