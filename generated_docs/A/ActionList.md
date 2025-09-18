# ActionList

## Location
[src/backend/commands/async.c:345-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L345-L350)

## Overview
ActionList is a structure that maintains transaction-aware state for LISTEN/NOTIFY actions, supporting nested transactions and proper rollback behavior.

## Definition


## Detailed Description
The ActionList structure implements a transaction-aware stack for managing LISTEN and UNLISTEN operations within PostgreSQL's asynchronous notification system. It supports nested transactions (subtransactions) by maintaining a linked list structure where each level corresponds to a transaction nesting level. This design enables proper rollback behavior - if a subtransaction is aborted, only the actions performed at that level are undone, while preserving actions from outer transaction levels. The actions field contains a list of ListenAction structures that represent the specific LISTEN/UNLISTEN operations performed at this transaction level.

## Parameters / Member Variables
- : Integer representing the current transaction nesting depth (0 for top-level transactions, higher numbers for subtransactions)
- : Pointer to a List containing ListenAction structures representing the LISTEN/UNLISTEN operations performed at this transaction level
- : Pointer to the ActionList structure for the parent transaction level, forming a linked list of transaction levels

## Dependencies
- Functions called/Symbols referenced:
  - [ActionList](ActionList.md) (self-reference for the linked list structure)
- Called from (representative examples):
  - [queue_listen](../q/queue_listen.md)
  - [AtSubCommit_Notify](AtSubCommit_Notify.md)
  - [AtSubAbort_Notify](AtSubAbort_Notify.md)

## Notes and Other Information
- Part of PostgreSQL's transaction-aware LISTEN/NOTIFY system
- Supports nested transactions (subtransactions) with proper rollback semantics
- Forms a stack-like structure with newer transaction levels pointing to older ones via the upper field
- Works in conjunction with ListenAction structures to track specific operations
- Essential for maintaining ACID properties in the asynchronous notification system
- Used by transaction commit and abort handlers to apply or roll back notification subscriptions
- The nestingLevel field helps track transaction depth for proper cleanup and rollback operations