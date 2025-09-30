# nextval_internal

## Location
[src/backend/commands/sequence.c:623-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L623-L865)

## Overview
The core implementation function that generates the next value from a PostgreSQL sequence, handling caching, WAL logging, and all sequence state management.

## Definition
```c
int64 nextval_internal(Oid relid, bool check_permissions)
```

## Detailed Description
This function is the heart of PostgreSQL's sequence value generation system. It implements sophisticated sequence value caching, WAL logging optimizations, and proper transaction handling for sequences. The function handles both ascending and descending sequences, supports cycling, and implements an efficient caching mechanism to reduce WAL overhead.

Key features include:
- Multi-value caching to reduce WAL logging frequency
- Support for both cyclic and non-cyclic sequences  
- Proper handling of sequence limits (MAXVALUE/MINVALUE)
- WAL logging with checkpoint awareness
- Permission checking and transaction safety
- Protection against parallel execution and read-only transactions

The function uses a two-phase approach: first it checks if cached values are available, and if not, it fetches a new batch of values from the sequence relation, potentially logging some of them to WAL for crash recovery.

## Parameters / Member Variables
- `relid`: The OID of the sequence relation to operate on
- `check_permissions`: Whether to verify ACL_USAGE and ACL_UPDATE permissions for the sequence

## Dependencies
- Functions called/Symbols referenced:
  - [init_sequence](../i/init_sequence.md) (sequence initialization and locking)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) (permission verification)
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)/PreventCommandIfParallelMode (safety checks)
  - [SearchSysCache1](../S/SearchSysCache1.md) (sequence metadata lookup)
  - [read_seq_tuple](../r/read_seq_tuple.md) (sequence tuple reading)
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)/PageGetLSN (WAL checkpoint handling)
  - RelationNeedsWAL/GetTopTransactionId (WAL logging setup)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterBuffer/XLogInsert (WAL logging)
  - [MarkBufferDirty](../M/MarkBufferDirty.md) (buffer management)
  - [sequence_close](../s/sequence_close.md) (resource cleanup)
- Called from (representative examples):
  - [nextval](nextval.md) (text-based sequence interface)
  - [nextval_oid](nextval_oid.md) (OID-based sequence interface) 
  - [ExecEvalNextValueExpr](../E/ExecEvalNextValueExpr.md) (executor for nextval expressions)

## Notes and Other Information
- Implements sequence value caching to reduce WAL log volume (SEQ_LOG_VALS optimization)
- Uses critical sections around buffer modifications to ensure consistency
- Handles checkpoint boundaries by checking page LSN against redo pointer
- Supports both temp sequences (no WAL) and persistent sequences (with WAL)
- Maintains backend-local sequence cache in SeqTable for performance
- Prevents use in parallel query execution due to cache sharing limitations
- Returns int64 values supporting PostgreSQL's full bigint sequence range
- Central to PostgreSQL's sequence performance through intelligent caching and WAL management

## Simplified Source

```c
int64 nextval_internal(Oid relid, bool check_permissions) {
    SeqTable elm;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqdatatuple;
    Form_pg_sequence_data seq;
    int64 incby, maxv, minv, cache, log, fetch, last;
    int64 result, next, rescnt = 0;
    bool cycle, logit = false;

    // Initialize sequence and check permissions
    init_sequence(relid, &elm, &seqrel);

    if (check_permissions &&
        pg_class_aclcheck(elm->relid, GetUserId(), ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));
    }

    // Safety checks for read-only and parallel execution
    if (!seqrel->rd_islocaltemp)
        PreventCommandIfReadOnly("nextval()");
    PreventCommandIfParallelMode("nextval()");

    // Check cache for available values
    if (elm->last != elm->cached) {
        Assert(elm->last_valid && elm->increment != 0);
        elm->last += elm->increment;
        sequence_close(seqrel, NoLock);
        last_used_seq = elm;
        return elm->last;
    }

    // Read sequence metadata from system catalog
    HeapTuple pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(pgstuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);

    Form_pg_sequence pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
    incby = pgsform->seqincrement;
    maxv = pgsform->seqmax;
    minv = pgsform->seqmin;
    cache = pgsform->seqcache;
    cycle = pgsform->seqcycle;
    ReleaseSysCache(pgstuple);

    // Read current sequence state
    seq = read_seq_tuple(seqrel, &buf, &seqdatatuple);
    last = next = result = seq->last_value;
    fetch = cache;
    log = seq->log_cnt;

    if (!seq->is_called) {
        rescnt++;
        fetch--;
    }

    // Determine if WAL logging is needed
    if (log < fetch || !seq->is_called) {
        fetch = log = fetch + SEQ_LOG_VALS;
        logit = true;
    } else {
        XLogRecPtr redoptr = GetRedoRecPtr();
        if (PageGetLSN(BufferGetPage(buf)) <= redoptr) {
            fetch = log = fetch + SEQ_LOG_VALS;
            logit = true;
        }
    }

    // Generate sequence values up to cache limit
    while (fetch) {
        // Check bounds for ascending/descending sequences
        if (incby > 0) {
            // Ascending sequence: check MAXVALUE
            if ((maxv >= 0 && next > maxv - incby) || (maxv < 0 && next + incby > maxv)) {
                if (rescnt > 0) break;
                if (!cycle)
                    ereport(ERROR, (errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
                            errmsg("nextval: reached maximum value of sequence \"%s\" (%lld)",
                                   RelationGetRelationName(seqrel), (long long) maxv)));
                next = minv;
            } else {
                next += incby;
            }
        } else {
            // Descending sequence: check MINVALUE
            if ((minv < 0 && next < minv - incby) || (minv >= 0 && next + incby < minv)) {
                if (rescnt > 0) break;
                if (!cycle)
                    ereport(ERROR, (errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
                            errmsg("nextval: reached minimum value of sequence \"%s\" (%lld)",
                                   RelationGetRelationName(seqrel), (long long) minv)));
                next = maxv;
            } else {
                next += incby;
            }
        }

        fetch--;
        if (rescnt < cache) {
            log--;
            rescnt++;
            last = next;
            if (rescnt == 1)
                result = next;
        }
    }

    log -= fetch;
    Assert(log >= 0);

    // Update local cache
    elm->increment = incby;
    elm->last = result;
    elm->cached = last;
    elm->last_valid = true;
    last_used_seq = elm;

    // WAL logging if required
    if (logit && RelationNeedsWAL(seqrel))
        GetTopTransactionId();

    START_CRIT_SECTION();
    MarkBufferDirty(buf);

    // Write WAL record if needed
    if (logit && RelationNeedsWAL(seqrel)) {
        xl_seq_rec xlrec;
        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        seq->last_value = next;
        seq->is_called = true;
        seq->log_cnt = 0;

        xlrec.locator = seqrel->rd_locator;
        XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

        XLogRecPtr recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);
        PageSetLSN(BufferGetPage(buf), recptr);
    }

    // Update final sequence state
    seq->last_value = last;
    seq->is_called = true;
    seq->log_cnt = log;

    END_CRIT_SECTION();
    UnlockReleaseBuffer(buf);
    sequence_close(seqrel, NoLock);

    return result;
}
```