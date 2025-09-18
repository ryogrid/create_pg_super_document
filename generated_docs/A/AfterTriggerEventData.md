# AfterTriggerEventData

## Location
src/backend/commands/trigger.c: 3730 - 3743

## Overview
AfterTriggerEventData is the core data structure that stores information about deferred trigger events, including status flags, tuple identifiers, and partition OIDs for cross-partition operations.

## Definition
```c
typedef struct AfterTriggerEventData
{
    TriggerFlags ate_flags;     /* status bits and offset to shared data */
    ItemPointerData ate_ctid1;  /* inserted, deleted, or old updated tuple */
    ItemPointerData ate_ctid2;  /* new updated tuple */
    
    /*
     * During a cross-partition update of a partitioned table, we also store
     * the OIDs of source and destination partitions that are needed to fetch
     * the old (ctid1) and the new tuple (ctid2) from, respectively.
     */
    Oid         ate_src_part;
    Oid         ate_dst_part;
} AfterTriggerEventData;
```

## Detailed Description
AfterTriggerEventData is the fundamental data structure used in PostgreSQL's deferred trigger execution system. It stores all necessary information about a trigger event that needs to be executed later, including status information, tuple identifiers, and partition information for cross-partition operations.

The structure is designed to handle various types of trigger events efficiently:
- INSERT operations (uses ate_ctid1 for the new tuple)
- DELETE operations (uses ate_ctid1 for the deleted tuple)  
- UPDATE operations (uses both ate_ctid1 for old tuple, ate_ctid2 for new tuple)
- Cross-partition UPDATE operations (additionally uses ate_src_part and ate_dst_part)

## Parameters / Member Variables
- `ate_flags`: Status bits indicating trigger state (DONE/IN_PROGRESS) and type information, plus offset to shared trigger data
- `ate_ctid1`: Item pointer to the inserted, deleted, or old updated tuple
- `ate_ctid2`: Item pointer to the new updated tuple (used only for UPDATE operations)
- `ate_src_part`: OID of the source partition (used for cross-partition updates)
- `ate_dst_part`: OID of the destination partition (used for cross-partition updates)

## Dependencies
- Functions called/Symbols referenced:
  - TriggerFlags (for ate_flags member)
- Called from (representative examples):
  - AfterTriggerEvent (pointer typedef)
  - SizeofTriggerEvent (macro for size calculation)
  - AfterTriggerSaveEvent

## Notes and Other Information
- This is the full-sized event structure containing all possible fields
- For memory efficiency, PostgreSQL uses smaller variant structures (NoOids, OneCtid, ZeroCtids) when not all fields are needed
- The ate_src_part and ate_dst_part fields are specifically for handling row movement between partitions in partitioned tables
- Part of PostgreSQL's sophisticated trigger deferral system that allows constraint checking to be postponed until transaction commit