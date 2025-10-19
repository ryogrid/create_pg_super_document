# GetSlotInvalidationCause

## Location
[src/backend/replication/slot.c:2405-2432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2405-L2432)

## Overview
Maps an invalidation reason string for a replication slot to its corresponding ReplicationSlotInvalidationCause enum value.

## Definition

```c
ReplicationSlotInvalidationCause
GetSlotInvalidationCause(const char *invalidation_reason)
```
## Detailed Description
This function takes a string representation of a replication slot invalidation reason and converts it to the corresponding ReplicationSlotInvalidationCause enum value. It performs this mapping by iterating through all possible invalidation causes and comparing the input string with the string representations stored in the SlotInvalidationCauses array. The function includes assertions to ensure that a valid invalidation reason is provided and that a matching cause is found.

## Parameters / Member Variables
- `invalidation_reason`: A string representing the reason why a replication slot was invalidated

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotInvalidationCause](../R/ReplicationSlotInvalidationCause.md) (enum type)
  - RS_INVAL_NONE (enum constant)
  - RS_INVAL_MAX_CAUSES (enum constant) 
  - SlotInvalidationCauses (array for string lookup)
  - PG_USED_FOR_ASSERTS_ONLY (macro)
- Called from (representative examples):
  - SLOTSYNC_COLUMN_COUNT (in slotsync.c)

## Notes and Other Information
- The function uses assertions to validate input and ensure a match is found, making it suitable for debug builds
- This is a utility function that provides a reverse mapping from string to enum, typically used when deserializing or parsing invalidation reasons
- The function assumes that the SlotInvalidationCauses array contains string representations corresponding to each enum value

## Simplified Source

```c
ReplicationSlotInvalidationCause GetSlotInvalidationCause(const char *invalidation_reason)
{
    ReplicationSlotInvalidationCause cause;
    ReplicationSlotInvalidationCause result = RS_INVAL_NONE;

    // Search through all possible invalidation causes
    for (cause = RS_INVAL_NONE; cause <= RS_INVAL_MAX_CAUSES; cause++)
    {
        // Compare input string with predefined cause strings
        if (strcmp(SlotInvalidationCauses[cause], invalidation_reason) == 0)
        {
            result = cause;
            break;
        }
    }

    return result;
}
```