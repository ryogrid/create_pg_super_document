# XLogPrefetcherReadRecord

## Location
[src/backend/access/transam/xlogprefetcher.c:983-1082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L983-L1082)

## Overview
XLogPrefetcherReadRecord is a wrapper function for XLogReadRecord() that provides the same interface while simultaneously initiating I/O prefetching for blocks referenced in future WAL records to improve recovery performance.

## Definition


## Detailed Description
This function serves as the main entry point for reading WAL records with prefetching optimization during recovery. It manages the prefetching machinery by:

1. **Configuration Management**: Checks if prefetching parameters have changed via GUC settings and reconfigures the streaming read subsystem accordingly
2. **Resource Management**: Releases previously returned records and completes any finished filters
3. **I/O Coordination**: Manages the lifecycle of prefetch operations using the Local Request Queue (LRQ) system
4. **Statistics**: Periodically computes and updates prefetching statistics

The function dynamically adjusts prefetching behavior based on the  setting and whether recovery prefetching is enabled. When prefetching is disabled, it falls back to minimal I/O concurrency.

## Parameters / Member Variables
- : Pointer to the XLogPrefetcher structure containing prefetching state and configuration
- : Pointer to a char pointer where error messages will be stored if the operation fails

## Dependencies
- Functions called/Symbols referenced:
  -  - Frees Local Request Queue resources
  -  - Checks if recovery prefetching is enabled
  -  - Allocates new Local Request Queue with specified parameters
  -  - Releases the previously returned WAL record
  -  - Completes any finished block filters
  -  - Marks I/O operations as completed up to a given LSN
  -  - Checks if records are queued for reading
  -  - Initiates prefetching operations
  -  - Reads the next WAL record
  -  - Computes prefetching statistics
- Called from (representative examples):
  -  in src/backend/access/transam/xlogrecovery.c:3151

## Notes and Other Information
- The function uses the  macro for performance optimization on configuration checks and statistics computation
- Prefetch distance is calculated as 
- The function maintains thread safety by properly managing the lifecycle of prefetch operations
- When  is set very low, some blocks referenced in the current record may not be prefetched
- Statistics are computed periodically based on the amount of WAL processed, not on every call
- The function is critical for recovery performance in PostgreSQL systems with high I/O workloads