# MVCC Quick Reference Card

> MVCC Documentation > Quick Reference

---

## What MVCC Does

Readers never block writers. Writers never block readers. Each transaction sees a consistent snapshot of the database. Old tuple versions are garbage-collected by VACUUM.

## Key Data Structures

| Structure | Location | Purpose |
|-----------|----------|---------|
| `HeapTupleHeaderData` | `htup_details.h:153` | 23-byte tuple header: t_xmin, t_xmax, t_ctid, t_infomask |
| `SnapshotData` | `snapshot.h:142` | Snapshot: xmin, xmax, xip[], curcid |
| `PGPROC` | `proc.h:162` | Per-backend shared memory: xid, xmin, subxids |
| `PROC_HDR` | `proc.h:370` | Global dense arrays: xids[], subxidStates[] |

## Tuple Header Quick Reference

| Field | Meaning |
|-------|---------|
| `t_xmin` | XID that inserted this version |
| `t_xmax` | XID that deleted/updated/locked this version (0 = none) |
| `t_cid` | Command ID within the transaction |
| `t_ctid` | Points to self (latest) or next version (updated) |
| `t_infomask` | Hint bits + lock state + property flags |

## Infomask Quick Reference

| Flag | Value | Meaning |
|------|-------|---------|
| `HEAP_XMIN_COMMITTED` | 0x0100 | xmin committed (hint bit) |
| `HEAP_XMIN_INVALID` | 0x0200 | xmin aborted (hint bit) |
| `HEAP_XMIN_FROZEN` | 0x0300 | Both bits = frozen (always visible) |
| `HEAP_XMAX_COMMITTED` | 0x0400 | xmax committed (hint bit) |
| `HEAP_XMAX_INVALID` | 0x0800 | xmax invalid (not deleted) |
| `HEAP_XMAX_LOCK_ONLY` | 0x0080 | xmax is a lock, not a delete |
| `HEAP_XMAX_IS_MULTI` | 0x1000 | xmax is a MultiXactId |
| `HEAP_HOT_UPDATED` | 0x4000 | (infomask2) This tuple was HOT-updated |
| `HEAP_ONLY_TUPLE` | 0x8000 | (infomask2) Heap-only tuple, no index entry |

## Visibility Rule Summary

A tuple is visible if:
1. `t_xmin` committed BEFORE the snapshot was taken, AND
2. `t_xmax` is absent, uncommitted, or committed AFTER the snapshot

## Snapshot Boundaries

```
XIDs:    ...  [completed]  xmin  [in xip[]]  xmax  [not yet assigned]  ...
                  |                              |
           Visible if committed          Always invisible
```

## Critical Path Sequences

### Transaction Commit
```
CommitTransaction() -> RecordTransactionCommit()
  -> XactLogCommitRecord()   [WAL]
  -> XLogFlush()             [sync commit]
  -> TransactionIdCommitTree() [CLOG]
  -> ProcArrayEndTransaction() [clear shared state]
  -> Release locks + cleanup
```

### MVCC Visibility Check
```
HeapTupleSatisfiesVisibility() -> HeapTupleSatisfiesMVCC()
  -> Check xmin hint bits (fast path)
  -> XidInMVCCSnapshot(xmin) [snapshot check]
  -> TransactionIdDidCommit(xmin) [CLOG fallback]
  -> SetHintBits() [cache result]
  -> Check xmax (same pattern)
```

### VACUUM Dead Tuple Removal
```
vacuum_get_cutoffs() -> lazy_scan_heap()
  -> heap_page_prune_and_freeze()
    -> HeapTupleSatisfiesVacuumHorizon() [classify]
    -> heap_prepare_freeze_tuple() [freeze plan]
  -> lazy_vacuum() [index cleanup]
  -> lazy_vacuum_heap_rel() [LP_DEAD -> LP_UNUSED]
  -> vac_truncate_clog() [truncate CLOG]
```

## Isolation Level Behavior

| Level | Snapshot Policy | Conflict Handling |
|-------|----------------|-------------------|
| READ COMMITTED | New snapshot per statement | EPQ recheck on conflict |
| REPEATABLE READ | Single snapshot for transaction | ERROR on write conflict |
| SERIALIZABLE | Single snapshot + SSI | ERROR on rw-dependency cycle |

## CLOG Status Encoding

| Bits | Status | Meaning |
|------|--------|---------|
| 00 | IN_PROGRESS | Running or crash-aborted |
| 01 | COMMITTED | Committed |
| 10 | ABORTED | Explicitly aborted |
| 11 | SUB_COMMITTED | Subtransaction committed, parent pending |

## VACUUM Cutoff Thresholds

```
relfrozenxid  FreezeLimit  OldestXmin  nextXID
     |             |            |          |
     v             v            v          v
  ---+-------------+------------+----------+--->
     | Must freeze | May freeze | Cannot   |
     | (aggressive)| (opportun.)| freeze   |
```

## XID Wraparound Thresholds

| Threshold | Action |
|-----------|--------|
| `xidVacLimit` | Start aggressive autovacuum |
| `xidWarnLimit` | Issue WARNING (40M from wrap) |
| `xidStopLimit` | Refuse new XIDs (3M from wrap) |
| `xidWrapLimit` | Actual wraparound point |

## Special XIDs

| XID | Name | Meaning |
|-----|------|---------|
| 0 | InvalidTransactionId | No transaction |
| 1 | BootstrapTransactionId | initdb; always committed |
| 2 | FrozenTransactionId | Frozen; always visible |
| 3 | FirstNormalTransactionId | First regular XID |

## Common Debugging Tips

1. **Checking tuple visibility**: Use `pageinspect` extension's `heap_page_items()` to examine t_xmin, t_xmax, t_infomask.

2. **Identifying VACUUM issues**: Check `pg_stat_user_tables.n_dead_tup` and `last_autovacuum`. High dead tuple counts indicate VACUUM is falling behind.

3. **XID wraparound monitoring**: Check `pg_database.datfrozenxid` and compare with `txid_current()`. Alert when age exceeds `autovacuum_freeze_max_age`.

4. **Snapshot too old**: Long-running transactions hold back OldestXmin. Check `pg_stat_activity` for old xact_start values.

5. **ProcArray contention**: Monitor `pg_stat_activity` for backends waiting on `ProcArrayLock`. Consider reducing `max_connections` or optimizing commit frequency.

6. **HOT update ratio**: Check `pg_stat_user_tables.n_tup_hot_upd` vs `n_tup_upd`. Low ratio suggests indexes on frequently-updated columns.

---

Previous: [Appendix: Data Structures](appendix_data_structures.md) | Next: [API Reference](mvcc_api_reference.md)
