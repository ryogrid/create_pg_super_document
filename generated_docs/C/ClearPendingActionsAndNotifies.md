# ClearPendingActionsAndNotifies

## Location
[src/backend/commands/async.c:2387-2402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2387-L2402)

## Overview
Clears the pending actions and notifications lists by resetting global pointers, relying on PostgreSQL's memory context system for automatic memory cleanup.

## Definition
```c
static void ClearPendingActionsAndNotifies(void)
```

## Detailed Description
This function is responsible for clearing both the pendingActions and pendingNotifies global variables in PostgreSQL's asynchronous notification system. Rather than explicitly freeing memory or destroying data structures, it simply resets the pointers to NULL and relies on PostgreSQL's memory context management system for cleanup.

The function is designed with the understanding that all memory associated with pending actions and notifications is allocated within specific transaction contexts (either TopTransactionContext or subtransaction contexts). When these contexts are eventually deleted by the memory management system, all associated memory will be automatically reclaimed, making explicit deallocation unnecessary.

This approach is both efficient and safe, as it avoids potential double-free errors while ensuring that memory is properly reclaimed when the transaction context is cleaned up.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (only assigns NULL to global variables)
- Called from (representative examples):
  - [AtCommit_Notify](../A/AtCommit_Notify.md) (during transaction commit processing)
  - [AtAbort_Notify](../A/AtAbort_Notify.md) (during transaction abort processing)

## Notes and Other Information
- This is a static function internal to async.c
- The function relies on PostgreSQL's memory context system for actual memory deallocation
- Memory is allocated in TopTransactionContext or subtransaction-specific contexts
- No explicit memory freeing is performed, avoiding potential memory management issues
- Used during both normal commit and abort processing to clean up notification state