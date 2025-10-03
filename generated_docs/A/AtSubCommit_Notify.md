# AtSubCommit_Notify

## Location
[src/backend/commands/async.c:1691-1760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1691-L1760)

## Overview
Handles PostgreSQL LISTEN/NOTIFY operations during subtransaction commit by reassigning pending actions and notifications from the current subtransaction to its parent transaction.

## Definition
```c
void AtSubCommit_Notify(void)
```

## Detailed Description
This function is called during subtransaction commit to handle pending LISTEN/NOTIFY operations. It performs two main tasks: reassigning pending LISTEN/UNLISTEN actions and pending NOTIFY events from the current subtransaction level to the parent transaction level. This ensures that operations initiated in a subtransaction are properly preserved when the subtransaction commits successfully.

The function handles two data structures:
1. **pendingActions**: LISTEN/UNLISTEN commands that need to be processed
2. **pendingNotifies**: NOTIFY events that need to be delivered

For actions, the function either decrements the nesting level (simple reparenting) or merges action lists if there are intermediate transaction levels. For notifications, it performs similar reparenting but includes duplicate elimination to prevent "Assert(!found)" failures when building parent-level hash tables.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [list_concat](../l/list_concat.md)
  - [AsyncExistsPendingNotify](AsyncExistsPendingNotify.md)
  - [AddEventToPendingNotifies](AddEventToPendingNotifies.md)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)

## Notes and Other Information
- The function must handle duplicate elimination for notifications but not for actions (as noted in the comment referencing queue_listen())
- Uses assertion to verify that pendingNotifies are at the expected nesting level
- Part of PostgreSQL's subtransaction management system for asynchronous notifications
- Located in src/backend/commands/async.c:1691-1760

## Simplified Source

```c
void AtSubCommit_Notify(void)
{
    int my_level = GetCurrentTransactionNestLevel();

    // Handle pending LISTEN/UNLISTEN actions
    if (pendingActions != NULL &&
        pendingActions->nestingLevel >= my_level)
    {
        if (pendingActions->upper == NULL ||
            pendingActions->upper->nestingLevel < my_level - 1)
        {
            // Simple case: just reparent to parent
            --pendingActions->nestingLevel;
        }
        else
        {
            // Complex case: merge with parent's actions
            ActionList *childPendingActions = pendingActions;
            pendingActions = pendingActions->upper;

            // Don't eliminate duplicates for actions (see queue_listen())
            pendingActions->actions =
                list_concat(pendingActions->actions,
                           childPendingActions->actions);
            pfree(childPendingActions);
        }
    }

    // Handle pending NOTIFY events
    if (pendingNotifies != NULL &&
        pendingNotifies->nestingLevel >= my_level)
    {
        Assert(pendingNotifies->nestingLevel == my_level);

        if (pendingNotifies->upper == NULL ||
            pendingNotifies->upper->nestingLevel < my_level - 1)
        {
            // Simple case: just reparent to parent
            --pendingNotifies->nestingLevel;
        }
        else
        {
            // Complex case: merge with parent's notifications
            NotificationList *childPendingNotifies = pendingNotifies;
            ListCell *l;

            pendingNotifies = pendingNotifies->upper;

            // Insert child events into parent, eliminating duplicates
            foreach(l, childPendingNotifies->events)
            {
                Notification *childn = (Notification *) lfirst(l);

                if (!AsyncExistsPendingNotify(childn))
                    AddEventToPendingNotifies(childn);
            }
            pfree(childPendingNotifies);
        }
    }
}
```