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

## Simplified Source
```c
Datum pg_lock_status(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    PG_Lock_Status *mystatus;
    LockData *lockData;
    PredicateLockData *predLockData;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        // Initialize function context for multiple calls
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Create tuple descriptor with 16 columns
        tupdesc = CreateTemplateTupleDesc(NUM_LOCK_STATUS_COLUMNS);
        TupleDescInitEntry(tupdesc, 1, "locktype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "database", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "relation", OIDOID, -1, 0);
        // ... initialize remaining columns ...
        TupleDescInitEntry(tupdesc, 16, "waitstart", TIMESTAMPTZOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        // Collect all lock information
        mystatus = palloc(sizeof(PG_Lock_Status));
        funcctx->user_fctx = mystatus;
        mystatus->lockData = GetLockStatusData();
        mystatus->currIdx = 0;
        mystatus->predLockData = GetPredicateLockStatusData();
        mystatus->predLockIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    mystatus = (PG_Lock_Status *) funcctx->user_fctx;
    lockData = mystatus->lockData;

    // Process regular locks
    while (mystatus->currIdx < lockData->nelements) {
        bool granted;
        LOCKMODE mode = 0;
        const char *locktypename;
        Datum values[NUM_LOCK_STATUS_COLUMNS] = {0};
        bool nulls[NUM_LOCK_STATUS_COLUMNS] = {0};
        HeapTuple tuple;
        Datum result;
        LockInstanceData *instance;

        instance = &(lockData->locks[mystatus->currIdx]);

        // Check for held lock modes
        granted = false;
        if (instance->holdMask) {
            for (mode = 0; mode < MAX_LOCKMODES; mode++) {
                if (instance->holdMask & LOCKBIT_ON(mode)) {
                    granted = true;
                    instance->holdMask &= LOCKBIT_OFF(mode);
                    break;
                }
            }
        }

        // If no held modes, check for waiting
        if (!granted) {
            if (instance->waitLockMode != NoLock) {
                mode = instance->waitLockMode;
                mystatus->currIdx++;
            }
            else {
                mystatus->currIdx++;
                continue;
            }
        }

        // Format lock information based on lock type
        if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
            locktypename = LockTagTypeNames[instance->locktag.locktag_type];
        else
            locktypename = "unknown";

        values[0] = CStringGetTextDatum(locktypename);

        // Set appropriate fields based on lock type
        switch ((LockTagType) instance->locktag.locktag_type) {
            case LOCKTAG_RELATION:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                // Set nulls for unused fields
                break;
            case LOCKTAG_TRANSACTION:
                values[6] = TransactionIdGetDatum(instance->locktag.locktag_field1);
                // Set nulls for unused fields
                break;
            // ... handle other lock types ...
        }

        // Set common fields
        values[10] = VXIDGetDatum(instance->vxid.procNumber, instance->vxid.localTransactionId);
        if (instance->pid != 0)
            values[11] = Int32GetDatum(instance->pid);
        else
            nulls[11] = true;

        values[12] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, mode));
        values[13] = BoolGetDatum(granted);
        values[14] = BoolGetDatum(instance->fastpath);

        if (!granted && instance->waitStart != 0)
            values[15] = TimestampTzGetDatum(instance->waitStart);
        else
            nulls[15] = true;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    // Process predicate locks
    predLockData = mystatus->predLockData;
    if (mystatus->predLockIdx < predLockData->nelements) {
        PredicateLockTargetType lockType;
        PREDICATELOCKTARGETTAG *predTag = &(predLockData->locktags[mystatus->predLockIdx]);
        SERIALIZABLEXACT *xact = &(predLockData->xacts[mystatus->predLockIdx]);
        Datum values[NUM_LOCK_STATUS_COLUMNS] = {0};
        bool nulls[NUM_LOCK_STATUS_COLUMNS] = {0};
        HeapTuple tuple;
        Datum result;

        mystatus->predLockIdx++;

        // Format predicate lock information
        lockType = GET_PREDICATELOCKTARGETTAG_TYPE(*predTag);
        values[0] = CStringGetTextDatum(PredicateLockTagTypeNames[lockType]);

        // Set target information
        values[1] = GET_PREDICATELOCKTARGETTAG_DB(*predTag);
        values[2] = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag);
        // ... set page and tuple fields based on lock type ...

        // Set holder information
        values[10] = VXIDGetDatum(xact->vxid.procNumber, xact->vxid.localTransactionId);
        if (xact->pid != 0)
            values[11] = Int32GetDatum(xact->pid);
        else
            nulls[11] = true;

        // Predicate locks are always SIReadLocks, granted, no fastpath
        values[12] = CStringGetTextDatum("SIReadLock");
        values[13] = BoolGetDatum(true);
        values[14] = BoolGetDatum(false);
        nulls[15] = true;

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}
```