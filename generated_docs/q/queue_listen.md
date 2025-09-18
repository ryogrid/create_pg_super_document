# queue_listen

## Location
src/backend/commands/async.c: 690 - 737

## Overview
Common internal function for LISTEN, UNLISTEN, and UNLISTEN ALL commands that adds listen action requests to the pending actions list for execution during transaction commit.

## Definition


## Detailed Description
queue_listen serves as the shared implementation for all listening-related SQL commands (LISTEN, UNLISTEN, UNLISTEN ALL). Rather than immediately updating the listenChannels list, it queues the action for deferred execution during transaction commit. This ensures proper transactional semantics where listen/unlisten operations only take effect if the transaction successfully commits. The function manages actions hierarchically across transaction nesting levels and does not attempt to optimize by collapsing duplicate or conflicting actions, as the interaction semantics would be too complex to guarantee correctness.

## Parameters / Member Variables
- : The type of listen action to perform (ListenActionKind enum: LISTEN, UNLISTEN, or UNLISTEN_ALL)
- : The notification channel name for LISTEN/UNLISTEN actions (ignored for UNLISTEN_ALL)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - MemoryContextSwitchTo
  - MemoryContextAlloc
  - palloc
  - strcpy
  - list_make1
  - lappend
  - ListenAction (struct)
  - ActionList (struct)
  - ListenActionKind (enum)
- Called from (representative examples):
  - Async_Listen
  - Async_Unlisten
  - Async_UnlistenAll

## Notes and Other Information
- Static function - internal to async.c module
- Uses CurTransactionContext for action record storage to ensure proper lifetime
- Creates hierarchical action lists based on transaction nesting levels  
- Does not perform deduplication or conflict resolution of actions
- Action execution is deferred until transaction commit via commit hooks
- Allocates ActionList in TopTransactionContext to handle nesting level changes during subtransaction commit