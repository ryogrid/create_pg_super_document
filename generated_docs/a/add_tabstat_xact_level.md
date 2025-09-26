# add_tabstat_xact_level

## Location
src/backend/utils/activity/pgstat_relation.c: 917 - 943

## Overview
Creates a new transaction state record for tracking table statistics at a specific transaction nesting level, establishing the necessary data structures to track table modifications within savepoints and subtransactions.

## Definition


## Detailed Description
This function creates and initializes a new  structure to track table statistics changes at a specific transaction nesting level. It manages the hierarchical transaction state stack by:

1. Ensuring the transaction stack level exists by calling 
2. Allocating a new  structure in the top transaction context
3. Linking the new transaction state into both the per-table transaction chain and the global transaction stack
4. Setting up proper parent-child relationships for nested transaction rollback support

The function is essential for PostgreSQL's statistics tracking system to properly handle savepoints and subtransactions, allowing statistics changes to be rolled back when subtransactions abort.

## Parameters / Member Variables
- : Pointer to the table's statistics status structure that needs transaction-level tracking
- : The transaction nesting level (0 for main transaction, higher values for savepoints/subtransactions)

## Dependencies
- Functions called/Symbols referenced:
  - : Ensures the transaction stack level exists
  - : Allocates zeroed memory in TopTransactionContext
  - : Transaction-specific table statistics structure
  - : Per-subtransaction state structure
  - : Main table statistics tracking structure
- Called from (representative examples):
  - : The primary caller that determines when new transaction levels are needed

## Notes and Other Information
- The function is static and only used internally within the statistics relation module
- Memory allocation uses TopTransactionContext to ensure proper cleanup when transactions end
- The linking strategy maintains both forward (next) and backward (upper) chains for efficient traversal
- This is part of PostgreSQL's sophisticated statistics tracking system that must handle complex transaction scenarios including nested savepoints