# ListenAction

## Location
src/backend/commands/async.c: 343 - 344

## Overview
ListenAction is a structure that represents a queued LISTEN/NOTIFY operation, containing both the action type and the channel name for deferred execution during transaction commit.

## Definition


## Detailed Description
ListenAction is a flexible-sized structure used to queue LISTEN, UNLISTEN, and UNLISTEN ALL operations during transaction execution. Each instance represents a single pending operation that will be processed when the transaction commits. The structure uses a flexible array member for the channel name to efficiently store channel names of varying lengths without requiring separate memory allocations. This design is part of PostgreSQL's mechanism to ensure that LISTEN/NOTIFY operations have proper transactional semantics.

## Parameters / Member Variables
- : A ListenActionKind value indicating the type of operation (LISTEN, UNLISTEN, or UNLISTEN_ALL)
- : A flexible array member containing the null-terminated channel name string

## Dependencies
- Functions called/Symbols referenced:
  - [ListenActionKind](ListenActionKind.md) (for the action field type)
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for flexible array members)
- Called from (representative examples):
  - [queue_listen](../q/queue_listen.md) (src/backend/commands/async.c:693, 705)
  - [PreCommit_Notify](../P/PreCommit_Notify.md) (src/backend/commands/async.c:876)
  - [AtCommit_Notify](../A/AtCommit_Notify.md) (src/backend/commands/async.c:987)

## Notes and Other Information
- The structure uses a flexible array member to store channel names efficiently, avoiding separate memory allocations
- Memory allocation for ListenAction instances uses offsetof() to calculate the correct size including the channel name
- Instances are allocated in CurTransactionContext to ensure proper cleanup on transaction abort
- The structure is part of a linked list of pending actions maintained per transaction nesting level
- Channel names are limited by PostgreSQL's identifier length limits
- Defined at src/backend/commands/async.c:339-343