# FreeSubscription

## Location
src/backend/catalog/pg_subscription.c: 155 - 168

## Overview
Frees all memory allocated for a Subscription structure and its associated string and list data members.

## Definition
```c
void FreeSubscription(Subscription *sub)
```

## Detailed Description
FreeSubscription performs comprehensive cleanup of a Subscription structure by deallocating all dynamically allocated memory associated with it. This includes freeing string fields (name, conninfo, and optionally slotname), the publications list using deep free to handle nested string allocations, and finally the Subscription structure itself. The function properly handles the optional slotname field which may be NULL.

## Parameters / Member Variables
- `sub`: Pointer to the Subscription structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [list_free_deep](../l/list_free_deep.md) (deep free for list structures with allocated elements)
- Called from (representative examples):
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md) (logical replication worker cleanup)

## Notes and Other Information
- Essential counterpart to GetSubscription() for proper memory management
- Handles NULL check for optional slotname field before freeing
- Uses list_free_deep() for the publications list to ensure nested string allocations are properly freed
- Part of PostgreSQL's logical replication subscription management system
- Should be called whenever a Subscription structure allocated by GetSubscription() is no longer needed
- Follows PostgreSQL's memory management patterns using pfree() for all deallocations