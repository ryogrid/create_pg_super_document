# AsyncExistsPendingNotify

## Location
[src/backend/commands/async.c:2257-2297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2257-L2297)

## Overview
Checks whether a given notification already exists in the current transaction's pending notifications list, preventing duplicate notifications.

## Definition
```c
static bool AsyncExistsPendingNotify(Notification *n)
```

## Detailed Description
This function efficiently determines if a notification matching the provided one already exists in the pendingNotifies structure. It implements dual lookup strategies optimized for different scenarios: hash table lookup for larger notification sets, and linear list scanning for smaller sets.

When a hash table is available (indicating sufficient notification volume), it uses hash_search for O(1) lookup performance. Otherwise, it performs a linear scan through the events list, comparing both channel and payload lengths before doing a full memory comparison of the notification data to ensure complete matching.

## Parameters / Member Variables
- `n`: Pointer to the Notification structure to search for in pending notifications

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md): Performs hash table lookup for notification matching
  - HASH_FIND: Hash table operation mode constant
  - foreach/lfirst: List iteration macros for scanning notification events
  - memcmp: Memory comparison function for notification data matching
- Called from:
  - [Async_Notify](Async_Notify.md): Main NOTIFY command processing to prevent duplicates
  - [AtSubCommit_Notify](AtSubCommit_Notify.md): Subtransaction commit processing for notification deduplication

## Notes and Other Information
- Returns false immediately if pendingNotifies is NULL (no pending notifications)
- Uses adaptive lookup strategy based on presence of hash table in pendingNotifies structure
- [Hash](../H/Hash.md) table is typically created when notification volume exceeds a threshold
- Linear scan compares channel_len and payload_len before expensive memcmp operation
- Memory comparison includes both channel and payload data plus 2 bytes for null terminators
- Essential for preventing duplicate notifications within the same transaction
- Part of PostgreSQL's transaction-scoped notification deduplication system

## Simplified Source

```c
static bool
AsyncExistsPendingNotify(Notification *n)
{
    // No pending notifications at all
    if (pendingNotifies == NULL)
        return false;

    // Use hash table for fast lookup if available
    if (pendingNotifies->hashtab != NULL)
    {
        if (hash_search(pendingNotifies->hashtab, &n, HASH_FIND, NULL))
            return true;
    }
    else
    {
        // Scan through list for smaller notification sets
        ListCell *l;
        foreach(l, pendingNotifies->events)
        {
            Notification *existing = (Notification *) lfirst(l);

            // Quick length check before expensive comparison
            if (n->channel_len == existing->channel_len &&
                n->payload_len == existing->payload_len &&
                memcmp(n->data, existing->data,
                       n->channel_len + n->payload_len + 2) == 0)
                return true;
        }
    }

    return false;
}
```