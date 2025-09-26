# TransamVariablesData

## Location
src/include/access/transam.h: 209 - 255

## Overview
A shared memory data structure that tracks OID and transaction ID (XID) assignment state across the PostgreSQL cluster, with different fields protected by different LWLocks.

## Definition


## Detailed Description
TransamVariablesData is a central shared memory structure that maintains critical state information for PostgreSQL's transaction and object ID management systems. The structure exists for largely historical reasons as a single struct containing fields protected by different LWLocks, rather than being split into separate structures.

The structure serves multiple purposes:
1. **OID Generation**: Tracks the next available Object ID and manages OID allocation batching
2. **Transaction ID Management**: Maintains the next transaction ID to assign and various XID-related limits
3. **Vacuum Control**: Stores thresholds that trigger autovacuum operations and warnings
4. **Transaction Completion Tracking**: Records the latest completed transaction and completion counts
5. **Commit Timestamp Management**: Tracks the range of XIDs for which commit timestamps are available
6. **CLOG Management**: Maintains the oldest XID that's safe to look up in the transaction status log

The structure is initialized in shared memory during server startup and accessed throughout the system for transaction and OID assignment operations.

## Parameters / Member Variables
- : Next Object ID to be assigned (protected by OidGenLock)
- : Number of OIDs available before requiring XLOG work (protected by OidGenLock)
- : Next transaction ID to be assigned as a FullTransactionId (protected by XidGenLock)
- : Cluster-wide minimum datfrozenxid value (protected by XidGenLock)
- : Threshold where autovacuum is forced to start (protected by XidGenLock)
- : Threshold where XID wraparound warnings begin (protected by XidGenLock)
- : Threshold where new XID assignment is refused (protected by XidGenLock)
- : Absolute limit where transaction wraparound occurs (protected by XidGenLock)
- : Database OID that has the minimum datfrozenxid (protected by XidGenLock)
- : Oldest XID for which commit timestamp is available (protected by CommitTsLock)
- : Newest XID for which commit timestamp is available (protected by CommitTsLock)
- : Most recent completed (committed or aborted) transaction (protected by ProcArrayLock)
- : Count of completed top-level transactions, used for snapshot optimization (protected by ProcArrayLock)
- : Oldest XID safe to look up in commit log (protected by XactTruncationLock)

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId (for nextXid and latestCompletedXid fields)
  - TransactionId (for various XID limit fields)
  - Oid (for nextOid and oldestXidDB fields)

- Called from (representative examples):
  - VarsupShmemInit (initialization in shared memory)
  - VarsupShmemSize (size calculation for shared memory allocation)
  - Various transaction and OID generation functions

## Notes and Other Information
- The structure exists in shared memory and is accessible to all PostgreSQL processes
- Different fields are protected by different LWLocks to minimize contention
- The xidWrapLimit and oldestXidDB fields are not "active" values but are used for generating informative messages
- The xactCompletionCount is always above 1 and helps optimize snapshot generation
- Critical for preventing transaction ID wraparound and managing cluster-wide transaction state
- The structure layout reflects PostgreSQL's evolution, maintaining fields that serve different but related purposes