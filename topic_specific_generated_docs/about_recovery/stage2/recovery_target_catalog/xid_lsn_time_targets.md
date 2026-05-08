# Recovery Target Catalog: `recovery_target_xid`, `recovery_target_lsn`, `recovery_target_time`

The three "value" recovery targets — recovery stops when a WAL
record's xid/LSN/time matches the configured value.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `recovery_target_xid`

| Field | Value |
|-------|-------|
| Type | string → `TransactionId` |
| Default | `""` (unset) |
| Allowed range | Valid `TransactionId` |
| Context | `PGC_POSTMASTER` (must restart to change) |
| Category | `WAL_RECOVERY_TARGET` |
| GUC table entry | `guc_tables.c:4016` |
| Backing store | `recovery_target_xid_string` → `recoveryTargetXid` |

### Hooks

* `check_recovery_target_xid` at `xlogrecovery.c:5012` — parses
  the string into a `TransactionId`; refuses to be set if another
  `recovery_target_*` is already armed (mutual exclusion via the
  `extra` pointer).
* `assign_recovery_target_xid` at `xlogrecovery.c:5035` — sets
  `recoveryTargetXid` and `recoveryTarget = RECOVERY_TARGET_XID`.

### Comparison logic

`recoveryStopsBefore` / `recoveryStopsAfter` extract the xid from
COMMIT/ABORT/COMMIT_PREPARED/ABORT_PREPARED records and compare:

```c
recordXid = (xact_info == COMMIT)            ? XLogRecGetXid(record) :
            (xact_info == COMMIT_PREPARED)   ? parsed.twophase_xid :
            (xact_info == ABORT)             ? XLogRecGetXid(record) :
            (xact_info == ABORT_PREPARED)    ? parsed.twophase_xid :
                                                /* not a transaction record */
                                                return false;

if (recoveryTarget == RECOVERY_TARGET_XID && recordXid == recoveryTargetXid)
    /* exclusive: stop in recoveryStopsBefore;
     * inclusive: stop in recoveryStopsAfter */
```

The match is **equality only**, never `>=`, because xids are
issued in transaction-start order, not commit order. A higher xid
may complete before a lower one.

### Inclusive vs exclusive

* `recovery_target_inclusive=true` (default): stop **after**
  applying the matching xact record. The transaction's effects
  are visible.
* `recovery_target_inclusive=false`: stop **before** applying.
  The transaction's effects are not visible.

### Interaction with `recovery_target_timeline`

Independent. The xid match must occur on the WAL stream of the
chosen TLI; if the xid isn't present on that TLI's history, the
recovery exhausts WAL and stops without ever matching.

### Example

```
recovery_target_xid = '12345'
recovery_target_inclusive = on
```

Recovery replays records up to and including the COMMIT/ABORT for
xid 12345, then `recoveryStopsAfter` returns true. The
`recovery_target_action` then dispatches.

---

## `recovery_target_lsn`

| Field | Value |
|-------|-------|
| Type | string → `XLogRecPtr` (pg_lsn input format) |
| Default | `""` (unset) |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4043` |
| Backing store | `recovery_target_lsn_string` → `recoveryTargetLSN` |

### Hooks

* `check_recovery_target_lsn` at `xlogrecovery.c:4812` —
  validates with `pg_lsn_in`.
* `assign_recovery_target_lsn` at `xlogrecovery.c:4835`.

### Comparison

`record->ReadRecPtr` (record start LSN). Stop predicate:

* `recoveryStopsBefore`: `!inclusive &&
   record->ReadRecPtr >= recoveryTargetLSN`.
* `recoveryStopsAfter`: `inclusive &&
   record->ReadRecPtr >= recoveryTargetLSN`.

The LSN check fires on **any record**, not just XACT — so
recovery_target_lsn is the most precise target type.

### Inclusive vs exclusive

* inclusive=true: stop after the first record whose start LSN is
  ≥ target. The "after" actually means "this record was
  applied".
* inclusive=false: stop before the first record whose start LSN
  is ≥ target.

### Example

```
recovery_target_lsn = '0/1A234567'
recovery_target_inclusive = off
```

Recovery stops just before applying the first record at LSN
≥ `0/1A234567`. The standby is at `lastReplayedEndRecPtr <
0/1A234567`.

---

## `recovery_target_time`

| Field | Value |
|-------|-------|
| Type | string → `TimestampTz` |
| Default | `""` (unset) |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4025` |
| Backing store | `recovery_target_time_string` → `recoveryTargetTime` |

### Hooks

* `check_recovery_target_time` at `xlogrecovery.c:4895` —
  validates timestamp format. Cannot do final `timestamptz_in`
  parse here because the timezone may not yet be loaded; defers
  to `validateRecoveryParameters`.
* `assign_recovery_target_time` at `xlogrecovery.c:4950`.
* `validateRecoveryParameters` (`xlogrecovery.c:1147-1153`) does
  the final `timestamptz_in`.

### Comparison

`recoveryStopsBefore` / `recoveryStopsAfter` use
`getRecordTimestamp(record, &recordXtime)` — extracts
`xact_time` from `xl_xact_parsed_commit` /
`xl_xact_parsed_abort`.

```c
if (getRecordTimestamp(record, &recordXtime) &&
    recoveryTarget == RECOVERY_TARGET_TIME)
{
    if (inclusive)
        stopsHere = (recordXtime > recoveryTargetTime);
    else
        stopsHere = (recordXtime >= recoveryTargetTime);
}
```

### Inclusive vs exclusive

* `recovery_target_inclusive=true`: stop **after** the last
  transaction whose `xact_time <= recoveryTargetTime`.
* `recovery_target_inclusive=false`: stop **before** the first
  transaction whose `xact_time >= recoveryTargetTime`.

The asymmetry is intentional: many transactions may share the
same commit timestamp (clock granularity), so the inclusive
variant guarantees you see them all, the exclusive variant
guarantees you see none.

### Example

```
recovery_target_time = '2024-06-15 14:30:00 UTC'
recovery_target_inclusive = on
```

Recovery applies all transactions with `xact_time <=
2024-06-15 14:30:00 UTC` and stops just before the next one.

---

## Common: post-stop dispatch

After any of these targets fires:

1. The redo loop sets `reachedRecoveryTarget = true` and breaks.
2. The `recovery_target_action` dispatch runs:
   * `RECOVERY_TARGET_ACTION_PAUSE` — `recoveryPausesHere(true)`.
   * `RECOVERY_TARGET_ACTION_PROMOTE` — fall through to
     `FinishWalRecovery`.
   * `RECOVERY_TARGET_ACTION_SHUTDOWN` — `proc_exit(3)`.

### Globals after stop

```c
recoveryStopAfter   /* true if stopped via recoveryStopsAfter */
recoveryStopXid     /* set for XID/TIME stop */
recoveryStopTime    /* set for TIME/XID stop */
recoveryStopLSN     /* set for LSN stop */
recoveryStopName    /* not set (NAME-target uses the name catalog file) */
```

These are visible via `pg_last_xact_replay_timestamp()` and
similar.

---

## Source references

* `src/backend/access/transam/xlogrecovery.c:5012-5035` — XID
  hooks
* `src/backend/access/transam/xlogrecovery.c:4812-4835` — LSN
  hooks
* `src/backend/access/transam/xlogrecovery.c:4895-4950` — TIME
  hooks (parser + assign)
* `src/backend/access/transam/xlogrecovery.c:1147-1153` — final
  TIME conversion in validateRecoveryParameters
* `src/backend/access/transam/xlogrecovery.c:2573` —
  recoveryStopsBefore
* `src/backend/access/transam/xlogrecovery.c:2726` —
  recoveryStopsAfter
* `src/backend/utils/misc/guc_tables.c:4016, :4025, :4043` — GUC
  table entries
