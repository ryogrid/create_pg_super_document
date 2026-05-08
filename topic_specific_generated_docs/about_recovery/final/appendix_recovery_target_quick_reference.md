# Appendix F — Recovery Target Quick Reference

[← Recovery Conflict Quick Reference](appendix_recovery_conflict_quick_reference.md) | [index](index.md) | [next: PGDATA Recovery Layout →](appendix_pgdata_recovery_layout.md)

---

One row per `recovery_target_*` / apply-delay GUC. For per-GUC
detail, see [19_recovery_target_catalog.md](19_recovery_target_catalog.md).
For the architecture, see
[07_recovery_target_system.md](07_recovery_target_system.md).

| GUC | Type | Default | Comparison field | Stop predicate (line in `xlogrecovery.c`) |
|-----|------|---------|------------------|---------------------------------|
| `recovery_target` | string | "" | `reachedConsistency` (when `"immediate"`) | `recoveryStopsBefore`: `recoveryTarget == IMMEDIATE && reachedConsistency` (`:2589`) |
| `recovery_target_xid` | string→TransactionId | "" | xid of COMMIT/ABORT/COMMIT_PREPARED/ABORT_PREPARED | `recoveryStopsBefore` (exclusive) / `recoveryStopsAfter` (inclusive): `recordXid == recoveryTargetXid` |
| `recovery_target_time` | string→TimestampTz | "" | `xact_time` from COMMIT/ABORT records | `recoveryStopsBefore`: `recordXtime >= target`; `recoveryStopsAfter`: `recordXtime > target` |
| `recovery_target_lsn` | string→XLogRecPtr | "" | `record->ReadRecPtr` (start LSN of any record) | both: `record->ReadRecPtr >= recoveryTargetLSN` |
| `recovery_target_name` | string (≤ 63 chars) | "" | `xl_restore_point.rp_name` of `XLOG_RESTORE_POINT` records (RM_XLOG, info 0x70) | `recoveryStopsAfter`: `strcmp(rp_name, recoveryTargetName) == 0` (`:2748`) — always inclusive |
| `recovery_target_timeline` | string ("latest"/"current"/N) | "latest" | (constraint, not predicate) | n/a — used by `tliInHistory` / `tliOfPointInHistory` to filter the WAL stream |
| `recovery_target_inclusive` | bool | true | n/a | Modulates whether the matching record is replayed before stopping (`recoveryStopsBefore` vs `recoveryStopsAfter`). Ignored for NAME (always inclusive) and IMMEDIATE (n/a). |
| `recovery_target_action` | enum (pause/promote/shutdown) | pause | n/a | Post-stop dispatch in `PerformWalRecovery` (`:1851-1869`). PAUSE demoted to SHUTDOWN if `hot_standby = off`. PAUSE falls through to PROMOTE on resume. |
| `recovery_min_apply_delay` | int (ms) | 0 | `xact_time` from COMMIT/COMMIT_PREPARED records | `recoveryApplyDelay` (`:2982`): wait until `xact_time + delay <= now` before applying |

## Mutual exclusion

At most **one** of `{xid, time, lsn, name, immediate}` may be set. The
per-GUC `check_*` hook consults the `extra` slot used by the other
hooks; if another target is armed, parsing fails with
`ERRCODE_INVALID_PARAMETER_VALUE`.

`recovery_target_timeline`, `recovery_target_inclusive`,
`recovery_target_action`, and `recovery_min_apply_delay` are
*independent* of the value targets and may be combined freely with
any of them.

## Resolution order

1. **Postmaster start.** Each `recovery_target_*` GUC is parsed by
   its `check_*` hook (mutual exclusion enforced).
2. **`InitWalRecovery`** calls `validateRecoveryParameters` which:
   * Resolves `recovery_target_timeline` (LATEST → `findNewestTimeLine`;
     NUMERIC → `existsTimeLineHistory` check; CONTROLFILE → use loaded value).
   * Final `timestamptz_in` for `recovery_target_time` (deferred from
     parser because timezone may not be loaded).
   * Demotes `recovery_target_action = pause` to `shutdown` if
     `hot_standby = off`.
3. **`PerformWalRecovery`**: `recoveryStopsBefore` /
   `recoveryStopsAfter` consulted around each `ApplyWalRecord`.
4. **Stop fires**: post-stop dispatch runs (`recovery_target_action`).

## Sample `postgresql.conf` snippets

### PITR to a specific xid (exclusive — last good txn)

```ini
recovery_target_xid = '12345'
recovery_target_inclusive = off
recovery_target_action = 'pause'    # inspect, then resume to promote
```

### PITR to a moment in time (inclusive)

```ini
recovery_target_time = '2024-06-15 14:30:00 UTC'
recovery_target_inclusive = on
recovery_target_action = 'promote'
```

### PITR to a named restore point

```ini
recovery_target_name = 'preupgrade'   # set by SELECT pg_create_restore_point('preupgrade');
recovery_target_action = 'shutdown'   # inspect from a clean stop
```

### Standby with a one-hour apply delay

```ini
primary_conninfo = 'host=primary user=replicator'
recovery_min_apply_delay = '1h'
```
