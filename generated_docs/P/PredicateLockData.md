# PredicateLockData

## Location
src/include/storage/predicate_internals.h: 375 - 380

## Overview
PredicateLockData is a structure used to capture a snapshot of all predicate locks for reporting purposes, primarily serving the pg_locks view and related status functions.

## Definition


## Detailed Description
PredicateLockData serves as a container for capturing a complete snapshot of the predicate locking system's current state. It is specifically designed for diagnostic and monitoring purposes, primarily used by PostgreSQL's pg_lock_status function which feeds data to the pg_locks system view. The structure contains parallel arrays that store information about all active predicate locks at the time of capture, allowing external tools and administrators to inspect the current predicate lock state without interfering with the actual locking mechanisms. This read-only snapshot approach ensures that lock inspection does not impact the performance of serializable transactions.

## Parameters / Member Variables
- : Integer count of the total number of predicate lock entries captured in the snapshot
- : Array of PREDICATELOCKTARGETTAG structures, each identifying a specific database object being locked
- : Array of SERIALIZABLEXACT pointers, each identifying the serializable transaction holding the corresponding lock

## Dependencies
- Functions called/Symbols referenced:
  - PREDICATELOCKTARGETTAG
  - SERIALIZABLEXACT
- Called from (representative examples):
  - GetPredicateLockStatusData
  - pg_lock_status
  - predicatelock_hash

## Notes and Other Information
- Designed specifically for the pg_locks system view functionality
- Provides read-only snapshot of predicate lock state for monitoring and debugging
- Arrays locktags and xacts are parallel - element i in each array corresponds to the same predicate lock
- Used primarily by database administrators and monitoring tools to understand serializable transaction behavior
- Does not participate in actual predicate locking logic - purely for status reporting
- Memory for the arrays is typically allocated temporarily and freed after use
- Critical for troubleshooting serializable isolation level performance issues