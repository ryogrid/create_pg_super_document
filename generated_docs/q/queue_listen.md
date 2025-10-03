# queue_listen

## Location
[src/backend/commands/async.c:690-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L690-L737)

## Overview
Common internal function for LISTEN, UNLISTEN, and UNLISTEN ALL commands that adds listen action requests to the pending actions list for execution during transaction commit.

## Definition

```c
static void
queue_listen(ListenActionKind action, const char *channel)
```
## Detailed Description
queue_listen serves as the shared implementation for all listening-related SQL commands (LISTEN, UNLISTEN, UNLISTEN ALL). Rather than immediately updating the listenChannels list, it queues the action for deferred execution during transaction commit. This ensures proper transactional semantics where listen/unlisten operations only take effect if the transaction successfully commits. The function manages actions hierarchically across transaction nesting levels and does not attempt to optimize by collapsing duplicate or conflicting actions, as the interaction semantics would be too complex to guarantee correctness.

## Parameters / Member Variables
- `action`: The type of listen action to perform (ListenActionKind enum: LISTEN, UNLISTEN, or UNLISTEN_ALL)
- `*channel`: The notification channel name for LISTEN/UNLISTEN actions (ignored for UNLISTEN_ALL)
## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [palloc](../p/palloc.md)
  - strcpy
  - list_make1
  - [lappend](../l/lappend.md)
  - [ListenAction](../L/ListenAction.md) (struct)
  - [ActionList](../A/ActionList.md) (struct)
  - [ListenActionKind](../L/ListenActionKind.md) (enum)
- Called from (representative examples):
  - [Async_Listen](../A/Async_Listen.md)
  - [Async_Unlisten](../A/Async_Unlisten.md)
  - [Async_UnlistenAll](../A/Async_UnlistenAll.md)

## Notes and Other Information
- Static function - internal to async.c module
- Uses CurTransactionContext for action record storage to ensure proper lifetime
- Creates hierarchical action lists based on transaction nesting levels  
- Does not perform deduplication or conflict resolution of actions
- Action execution is deferred until transaction commit via commit hooks
- Allocates ActionList in TopTransactionContext to handle nesting level changes during subtransaction commit

## Simplified Source

```c
static void queue_listen(ListenActionKind action, const char *channel)
{
    MemoryContext oldcontext;
    ListenAction *action_record;
    int current_nesting_level = GetCurrentTransactionNestLevel();

    // Switch to transaction context for allocation
    oldcontext = MemoryContextSwitchTo(CurTransactionContext);

    // Create action record with channel name
    action_record = (ListenAction *) palloc(offsetof(ListenAction, channel) +
                                           strlen(channel) + 1);
    action_record->action = action;
    strcpy(action_record->channel, channel);

    // Check if we need a new action list for this nesting level
    if (pendingActions == NULL || current_nesting_level > pendingActions->nestingLevel)
    {
        ActionList *actions;

        // Create new action list in TopTransactionContext
        actions = (ActionList *) MemoryContextAlloc(TopTransactionContext, sizeof(ActionList));
        actions->nestingLevel = current_nesting_level;
        actions->actions = list_make1(action_record);
        actions->upper = pendingActions;
        pendingActions = actions;
    }
    else
    {
        // Add to existing action list
        pendingActions->actions = lappend(pendingActions->actions, action_record);
    }

    MemoryContextSwitchTo(oldcontext);
}
```