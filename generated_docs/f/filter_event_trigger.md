# filter_event_trigger

## Location
[src/backend/commands/event_trigger.c:594-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L594-L619)

## Overview
Determines whether a given event trigger should be fired based on session replication role and registered command tags.

## Definition
```c
static bool filter_event_trigger(CommandTag tag, EventTriggerCacheItem *item)
```

## Detailed Description
This function implements the filtering logic for event triggers, deciding whether an event trigger should fire for a specific command. It applies two main filters: session replication role filtering (to handle primary/replica scenarios) and command tag filtering (to match specific DDL commands). The function ensures that event triggers respect the replication configuration and only fire for the commands they are designed to handle.

## Parameters / Member Variables
- `tag`: CommandTag representing the DDL command being executed
- `item`: EventTriggerCacheItem containing the event trigger's configuration and properties

## Dependencies
- Functions called/Symbols referenced:
  - [EventTriggerCacheItem](../E/EventTriggerCacheItem.md) (cache structure type)
  - CommandTag (command identification type)
  - SessionReplicationRole (global replication role variable)
  - SESSION_REPLICATION_ROLE_REPLICA (replication role constant)
  - TRIGGER_FIRES_ON_ORIGIN (trigger firing mode constant)
  - TRIGGER_FIRES_ON_REPLICA (trigger firing mode constant)
  - bms_is_empty (bitmap set emptiness check)
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership test)
- Called from (representative examples):
  - [EventTriggerCommonSetup](../E/EventTriggerCommonSetup.md)

## Notes and Other Information
- This is a static internal function, not exposed outside event_trigger.c
- Implements two-level filtering: replication role and command tag matching
- Returns false to filter out (skip) the trigger, true to allow firing
- Handles the distinction between origin and replica firing modes for replication scenarios
- Uses bitmap sets (bms) for efficient command tag matching
- Assumes that disabled event triggers are already filtered out at a higher level
- Critical for ensuring event triggers behave correctly in replication environments

## Simplified Source

```c
// Simplified version of filter_event_trigger
static bool filter_event_trigger(CommandTag tag, EventTriggerCacheItem *item) {
    // Filter by session replication role
    if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA) {
        // In replica mode, skip triggers that only fire on origin
        if (item->enabled == TRIGGER_FIRES_ON_ORIGIN)
            return false;
    } else {
        // In origin mode, skip triggers that only fire on replica
        if (item->enabled == TRIGGER_FIRES_ON_REPLICA)
            return false;
    }

    // Filter by command tags if any were specified
    if (!bms_is_empty(item->tagset) && !bms_is_member(tag, item->tagset))
        return false;

    // All filters passed - allow this trigger to fire
    return true;
}
```

Key simplifications made:
- Added clarifying comments for each filtering step
- Simplified the replication role logic explanation
- Core filtering logic remains unchanged