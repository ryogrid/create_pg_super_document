# NotificationList

## Location
src/backend/commands/async.c: 389 - 395

## Overview
NotificationList manages the hierarchical structure of pending NOTIFY events across nested transactions and subtransactions, maintaining a linked list of notification events with optional hash table optimization for duplicate detection.

## Definition


## Detailed Description
NotificationList is the core data structure that manages pending NOTIFY events in PostgreSQL's asynchronous notification system across transaction boundaries. It implements a hierarchical system where each transaction nesting level maintains its own list of notifications, with successful subtransactions merging their notifications into their parent transaction's list.

The structure serves dual purposes: maintaining the ordered list of notifications to preserve delivery order guarantees, and providing efficient duplicate detection through an optional hash table. The hash table is only constructed when the number of pending notifications exceeds a threshold (MIN_HASHABLE_NOTIFIES), optimizing memory usage for transactions with few notifications.

The linked structure through the  field creates a stack-like organization where each subtransaction level can access its parent's notification state, enabling proper transaction semantics for the notification system.

## Parameters / Member Variables
- : The current transaction nesting depth, used to track subtransaction hierarchy
- : A PostgreSQL List containing Notification structures in the order they were issued
- : Hash table (HTAB) for efficient duplicate detection, created only when notification count exceeds threshold; NULL for small lists
- : Pointer to the parent transaction level's NotificationList, forming a linked hierarchy for nested transactions

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (PostgreSQL hash table type)
  - [List](../L/List.md) (PostgreSQL list implementation)
  - [NotificationList](NotificationList.md) (self-reference for hierarchy)
  
- Called from (representative examples):
  - NotificationHash (hash table operations)
  - [Async_Notify](../A/Async_Notify.md) (main notification processing function)
  - [AtSubCommit_Notify](../A/AtSubCommit_Notify.md) (subtransaction commit handling)
  - [AtSubAbort_Notify](../A/AtSubAbort_Notify.md) (subtransaction abort handling)

## Notes and Other Information
- Each subtransaction maintains its own NotificationList in its CurTransactionContext
- The  field creates a stack of notification lists corresponding to the transaction nesting levels
- Hash table optimization is applied only when notifications exceed MIN_HASHABLE_NOTIFIES to balance memory usage and lookup performance
- On subtransaction commit, the current level's events are merged with the parent level's list
- On subtransaction abort, the current level's NotificationList is simply discarded
- The structure ensures that notification delivery order matches the original NOTIFY command order
- Duplicate notifications within the same transaction are eliminated during the merging process
- The nestingLevel field helps maintain proper transaction semantics and enables correct cleanup on transaction abort