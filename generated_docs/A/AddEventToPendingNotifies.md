# AddEventToPendingNotifies

## Location
[src/backend/commands/async.c:2298-2356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2298-L2356)

## Overview
Adds a notification event to an existing pendingNotifies list and maintains an optional hash table for efficient duplicate detection when the list grows large enough.

## Definition

```c
struct NotificationHash);
```
## Detailed Description
This function is responsible for adding a notification event to the pending notifications list in PostgreSQL's asynchronous notification system. The function implements a performance optimization by creating a hash table when the number of pending notifications exceeds MIN_HASHABLE_NOTIFIES threshold (typically for lists with many entries). 

When the hash table is created for the first time, all existing notifications in the list are added to the hash table for fast lookup. The function ensures that notifications are added both to the ordered list (for proper delivery order) and to the hash table (for efficient duplicate detection).

The function assumes that pendingNotifies->events is already non-empty, which allows it to work correctly regardless of the current memory context.

## Parameters / Member Variables
- : A pointer to the Notification structure to be added to the pending notifications list

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to check if hash table creation is needed)
  - [hash_create](../h/hash_create.md) (to create the hash table)
  - [hash_search](../h/hash_search.md) (to add entries to the hash table)
  - [lappend](../l/lappend.md) (to add notification to the list)
  - [notification_hash](../n/notification_hash.md) (hash function for notifications)
  - [notification_match](../n/notification_match.md) (comparison function for notifications)
- Called from (representative examples):
  - [Async_Notify](Async_Notify.md)
  - [AtSubCommit_Notify](AtSubCommit_Notify.md)

## Notes and Other Information
- This is a static function internal to async.c
- The hash table is created lazily only when MIN_HASHABLE_NOTIFIES threshold is reached
- The function maintains both a list (for order) and hash table (for performance) data structures
- Uses CurTransactionContext for hash table memory allocation
- Asserts are used to ensure the notification doesn't already exist in the hash table

## Simplified Source

```c
static void
AddEventToPendingNotifies(Notification *n)
{
    Assert(pendingNotifies->events != NIL);

    // Create hash table when list gets large enough
    if (list_length(pendingNotifies->events) >= MIN_HASHABLE_NOTIFIES &&
        pendingNotifies->hashtab == NULL)
    {
        HASHCTL hash_ctl;
        ListCell *l;

        // Configure hash table
        hash_ctl.keysize = sizeof(Notification *);
        hash_ctl.entrysize = sizeof(struct NotificationHash);
        hash_ctl.hash = notification_hash;
        hash_ctl.match = notification_match;
        hash_ctl.hcxt = CurTransactionContext;

        // Create the hash table
        pendingNotifies->hashtab = hash_create("Pending Notifies", 256L,
                                             &hash_ctl,
                                             HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

        // Add all existing events to hash table
        foreach(l, pendingNotifies->events)
        {
            Notification *existing = (Notification *) lfirst(l);
            bool found;
            (void) hash_search(pendingNotifies->hashtab, &existing, HASH_ENTER, &found);
            Assert(!found);
        }
    }

    // Add new event to list
    pendingNotifies->events = lappend(pendingNotifies->events, n);

    // Add to hash table if it exists
    if (pendingNotifies->hashtab != NULL)
    {
        bool found;
        (void) hash_search(pendingNotifies->hashtab, &n, HASH_ENTER, &found);
        Assert(!found);
    }
}
```