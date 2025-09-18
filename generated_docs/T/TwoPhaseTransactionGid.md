# TwoPhaseTransactionGid

## Location
src/backend/replication/logical/worker.c: 4418 - 4437

## Overview
A static function that generates a standardized Global Transaction Identifier (GID) for two-phase commit transactions in logical replication.

## Definition
```c
static void TwoPhaseTransactionGid(Oid subid, TransactionId xid, char *gid, int szgid)
```

## Detailed Description
TwoPhaseTransactionGid is responsible for creating unique Global Transaction Identifiers (GIDs) used in PostgreSQL's two-phase commit protocol within logical replication. The function generates a standardized GID string that uniquely identifies a prepared transaction by combining the subscription ID and transaction ID.

The function performs validation to ensure both the subscription ID is valid (not InvalidRepOriginId) and the transaction ID is valid. If the transaction ID is invalid, it raises a protocol violation error. The generated GID follows the format "pg_gid_{subid}_{xid}", ensuring uniqueness across different subscriptions and transactions.

This GID is essential for the two-phase commit protocol, allowing the system to track and manage prepared transactions that span across multiple nodes in a distributed transaction scenario. The consistent naming format ensures that operations like commit prepared and rollback prepared can reliably identify the correct transaction.

## Parameters / Member Variables
- `subid`: Oid representing the subscription ID that identifies the logical replication subscription
- `xid`: TransactionId of the transaction for which the GID is being generated
- `gid`: Character buffer where the generated GID string will be stored
- `szgid`: Integer specifying the size of the GID buffer to prevent buffer overflow

## Dependencies
- Functions called/Symbols referenced:
  - InvalidRepOriginId (constant for checking valid replication origin)
  - TransactionIdIsValid (macro to validate transaction ID)
  - ereport (error reporting function)
  - snprintf (formatted string printing function)
- Called from (representative examples):
  - [apply_handle_prepare_internal](../a/apply_handle_prepare_internal.md) (at src/backend/replication/logical/worker.c:1083)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md) (at src/backend/replication/logical/worker.c:1180)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md) (at src/backend/replication/logical/worker.c:1229)

## Notes and Other Information
- Uses the format "pg_gid_{subid}_{xid}" for generated GIDs
- Includes validation to prevent invalid transaction IDs from being processed
- Essential component of PostgreSQL's two-phase commit implementation for logical replication
- The generated GID must be unique across the entire cluster to avoid conflicts
- Used in prepare, commit prepared, and rollback prepared operations to maintain transaction consistency