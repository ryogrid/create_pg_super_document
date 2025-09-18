# EnsurePortalSnapshotExists

## Location
src/backend/tcop/pquery.c: 1766 - 1793

## Overview
EnsurePortalSnapshotExists recreates a portal-level snapshot when none exists, typically after transactions have been committed or aborted within procedures or DO blocks.

## Definition


## Detailed Description
EnsurePortalSnapshotExists handles the restoration of MVCC snapshots for portal execution when the current snapshot has been destroyed by transaction control operations. This situation commonly occurs in stored procedures and DO blocks that can commit or abort transactions, thereby invalidating all existing snapshots.

The function implements a safety mechanism for SQL execution that ensures there's always an appropriate snapshot available when needed. It works by:
1. Checking if an active snapshot already exists (fast path)
2. Verifying that an active portal exists to provide context
3. Creating a new transaction snapshot and making it active
4. Associating the snapshot with the portal's transaction level

The function is critical for maintaining PostgreSQL's MVCC semantics in complex execution scenarios where transaction boundaries can occur within statement execution (like in procedures).

## Parameters / Member Variables
- None (operates on global portal and snapshot state)

## Dependencies
- Functions called/Symbols referenced:
  - ActiveSnapshotSet
  - GetTransactionSnapshot
  - PushActiveSnapshotWithLevel
  - GetActiveSnapshot
  - Portal structure and ActivePortal global
  - Portal fields (portalSnapshot, createLevel)
- Called from (representative examples):
  - ExecuteCallStmt
  - _SPI_execute_plan

## Notes and Other Information
- Only creates a snapshot if no active snapshot exists (optimization)
- Requires an active portal to provide transaction level context
- Associates the new snapshot with the portal's creation transaction level to prevent dangling pointers
- Critical for procedures and DO blocks that perform transaction control
- Works in conjunction with PlannedStmtRequiresSnapshot to manage snapshot lifecycle
- The snapshot creation uses the portal's createLevel to ensure proper snapshot management across nested transaction levels
- Throws an error if called without an active portal, as this indicates a programming error