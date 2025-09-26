# pg_lock_status

## Location
[src/backend/utils/adt/lockfuncs.c:93-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L93-L465)

## Overview
pg_lock_status is a PostgreSQL system function that produces a comprehensive view of all held and awaited locks in the database, returning detailed information about each lock mode as a set-returning function.

## Definition
Datum pg_lock_status(PG_FUNCTION_ARGS)

## Detailed Description
pg_lock_status is a set-returning function (SRF) that provides detailed visibility into PostgreSQL's locking subsystem. The function operates in two phases: first it iterates through all regular locks (relation, transaction, tuple, etc.), then through predicate locks used for serializable isolation. For each lock, it returns comprehensive information including lock type, target object, holder process, lock mode, grant status, and timing information. The function maintains state across multiple calls using FuncCallContext to efficiently process large lock tables. It handles various lock types including relation locks, page locks, tuple locks, transaction locks, virtual transaction locks, advisory locks, and serializable read locks.

## Parameters / Member Variables
This function takes no parameters (PG_FUNCTION_ARGS is the standard PostgreSQL function interface).

The function returns a tuple with 16 columns:
- `locktype`: Type of lock (relation, transaction, tuple, etc.)
- `database`: Database OID for the locked object
- `relation`: Relation OID for relation-based locks
- `page`: Page number for page-level locks
- `tuple`: Tuple offset for tuple-level locks
- `virtualxid`: Virtual transaction ID for VXID locks
- `transactionid`: Transaction ID for transaction locks
- `classid`: Class ID for object locks
- `objid`: Object ID for object locks
- `objsubid`: Object sub-ID for object locks
- `virtualtransaction`: Virtual transaction ID of the lock holder
- `pid`: Process ID of the lock holder
- `mode`: Lock mode name (e.g., AccessShareLock, ExclusiveLock)
- `granted`: Boolean indicating if the lock is granted or waiting
- `fastpath`: Boolean indicating if the lock uses the fastpath mechanism
- `waitstart`: Timestamp when lock waiting began (NULL if granted)

## Dependencies
- Functions called/Symbols referenced:
  - [GetLockStatusData](../G/GetLockStatusData.md) (retrieves current lock information)
  - [GetPredicateLockStatusData](../G/GetPredicateLockStatusData.md) (retrieves predicate lock information)
  - [VXIDGetDatum](../V/VXIDGetDatum.md) (formats virtual transaction IDs)
  - [GetLockmodeName](../G/GetLockmodeName.md) (converts lock mode to string)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (creates tuple descriptor)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) (initializes tuple descriptor columns)
  - [BlessTupleDesc](../B/BlessTupleDesc.md) (finalizes tuple descriptor)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates result tuples)
- Referenced types:
  - [LockData](../L/LockData.md), PredicateLockData, LockInstanceData
  - [PREDICATELOCKTARGETTAG](../P/PREDICATELOCKTARGETTAG.md), SERIALIZABLEXACT
  - [FuncCallContext](../F/FuncCallContext.md), PG_Lock_Status
- Called from:
  - SQL queries via pg_locks system view
  - Direct function calls in monitoring applications

## Notes and Other Information
- This function is the backend implementation for the pg_locks system view
- The function uses PostgreSQL's Set-Returning Function (SRF) framework for efficient memory management
- Lock information is gathered atomically at function start to ensure consistent snapshots
- The function handles both regular locks and predicate locks (used for serializable transactions)
- Different lock types populate different columns of the result set, with unused columns set to NULL
- The fastpath column indicates whether locks use PostgreSQL's optimization for frequently-acquired locks
- Wait timing information is only available for locks that are currently waiting
- The function processes locks destructively during iteration to avoid reporting the same lock mode multiple times