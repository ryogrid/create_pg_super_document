# AfterTriggerEventDataZeroCtids

## Location
src/backend/commands/trigger.c: 3761 - 3764

## Overview
AfterTriggerEventDataZeroCtids is the most space-optimized variant of AfterTriggerEventData, containing only trigger flags and used for foreign table triggers that store tuple data in a tuplestore.

## Definition
```c
/* AfterTriggerEventData, minus ate_*_part, ate_ctid1 and ate_ctid2 */
typedef struct AfterTriggerEventDataZeroCtids
{
    TriggerFlags ate_flags;     /* status bits and offset to shared data */
} AfterTriggerEventDataZeroCtids;
```

## Detailed Description
AfterTriggerEventDataZeroCtids represents the minimal trigger event structure in PostgreSQL's deferred trigger system. This structure contains only the essential trigger flags and is used specifically for foreign table triggers, which don't use CTIDs (since foreign tables don't have physical tuple identifiers like regular PostgreSQL tables).

For foreign table triggers, the actual tuple data is stored in a tuplestore, and the trigger flags include special markers like AFTER_TRIGGER_FDW_FETCH and AFTER_TRIGGER_FDW_REUSE to indicate how the tuple data should be retrieved during trigger execution:
- AFTER_TRIGGER_FDW_FETCH: Retrieve a fresh tuple from the tuplestore
- AFTER_TRIGGER_FDW_REUSE: Use the most recently retrieved tuple

This approach allows foreign table triggers to be fired in the exact order they were queued while minimizing memory usage.

## Parameters / Member Variables
- `ate_flags`: Status bits indicating trigger state (DONE/IN_PROGRESS) and type information, plus offset to shared trigger data

## Dependencies
- Functions called/Symbols referenced:
  - TriggerFlags (for ate_flags member)
- Called from (representative examples):
  - SizeofTriggerEvent (macro for size calculation)

## Notes and Other Information
- The most memory-efficient trigger event structure, saving 20 bytes compared to full AfterTriggerEventData
- Used exclusively for foreign table triggers where CTID information is not applicable
- Foreign table triggers are always non-deferrable to ensure proper tuple ordering from the tuplestore
- The tuplestore containing the actual tuple data is destroyed at the end of the query level
- Part of PostgreSQL's foreign data wrapper (FDW) trigger execution mechanism