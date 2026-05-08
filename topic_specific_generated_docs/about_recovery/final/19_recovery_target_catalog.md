# 19 — Recovery Target Catalog

[← Recovery Conflict Catalog](18_recovery_conflict_catalog.md) | [index](index.md) | [next: Deep Dives →](20_deep_dives.md)

---

This chapter is a per-GUC reference for the **9 `recovery_target_*`
and apply-delay GUCs** that control PITR / standby replay behavior.
Each entry follows the standardized template:

* **GUC name and basic facts** — type, default, context.
* **Parser / assign hooks** — `check_*` and `assign_*` functions.
* **Comparison field** — what part of the WAL record (or runtime
  state) is compared.
* **Stop predicate** — which line of `recoveryStopsBefore` /
  `recoveryStopsAfter` fires.
* **Inclusive semantics** — interaction with `recovery_target_inclusive`.
* **Post-stop action** — typically `recovery_target_action` dispatch.
* **Sample `postgresql.conf` snippet.**

For the architecture (`validateRecoveryParameters`,
`recoveryStopsBefore` / `recoveryStopsAfter`, `recoveryPausesHere`,
`recoveryApplyDelay`), see
[07_recovery_target_system.md](07_recovery_target_system.md).

For a one-line-per-GUC overview, see
[appendix_recovery_target_quick_reference.md](appendix_recovery_target_quick_reference.md).

## Catalog overview

| GUC | Type | Default | Section |
|-----|------|---------|---------|
| `recovery_target` | string ("" / "immediate") | "" | [§1](#1-recovery_target) |
| `recovery_target_xid` | string→TransactionId | "" | [§2](#2-recovery_target_xid) |
| `recovery_target_time` | string→TimestampTz | "" | [§3](#3-recovery_target_time) |
| `recovery_target_lsn` | string→XLogRecPtr | "" | [§4](#4-recovery_target_lsn) |
| `recovery_target_name` | string | "" | [§5](#5-recovery_target_name) |
| `recovery_target_timeline` | string ("latest" / "current" / N) | "latest" | [§6](#6-recovery_target_timeline) |
| `recovery_target_inclusive` | bool | true | [§7](#7-recovery_target_inclusive) |
| `recovery_target_action` | enum (pause/promote/shutdown) | pause | [§8](#8-recovery_target_action) |
| `recovery_min_apply_delay` | int (ms) | 0 | [§9](#9-recovery_min_apply_delay) |

---

## 1. `recovery_target`

| Field | Value |
|-------|-------|
| Type | string |
| Default | `""` |
| Allowed values | `""` (unset), `"immediate"` |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4007` |
| Backing store | `recovery_target_string` → `recoveryTarget` enum |

#### Hooks

* `check_recovery_target` at `xlogrecovery.c:4782` — only accepts
  `""` or `"immediate"`; mutual exclusion check.
* `assign_recovery_target` at `xlogrecovery.c:4796` — sets
  `recoveryTarget = RECOVERY_TARGET_IMMEDIATE`.

#### Comparison logic

In `recoveryStopsBefore` (`xlogrecovery.c:2589`):

```c
if (recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency)
{
    ereport(LOG, (errmsg("recovery stopping after reaching consistency")));
    return true;
}
```

The match fires the first record after `reachedConsistency`
becomes true (which happens when `lastReplayedEndRecPtr >=
minRecoveryPoint && backupEndPoint cleared`).

#### Use case

The minimum recovery — replay only as much as needed to make the
on-disk state consistent. Used for:

* Restoring a base backup quickly to an ad-hoc state.
* Diagnostic recoveries (don't replay further than the data
  itself requires).

#### Inclusive vs exclusive

Not applicable. The stop is "as soon as consistent" — there's no
inclusive/exclusive choice.

#### Example

```
recovery_target = 'immediate'
recovery_target_action = 'shutdown'
```

The cluster recovers the backup, reaches consistency, then exits
cleanly leaving `pg_control` in `DB_SHUTDOWNED_IN_RECOVERY`.

---

---

## 2. `recovery_target_xid`

| Field | Value |
|-------|-------|
| Type | string → `TransactionId` |
| Default | `""` (unset) |
| Allowed range | Valid `TransactionId` |
| Context | `PGC_POSTMASTER` (must restart to change) |
| Category | `WAL_RECOVERY_TARGET` |
| GUC table entry | `guc_tables.c:4016` |
| Backing store | `recovery_target_xid_string` → `recoveryTargetXid` |

#### Hooks

* `check_recovery_target_xid` at `xlogrecovery.c:5012` — parses
  the string into a `TransactionId`; refuses to be set if another
  `recovery_target_*` is already armed (mutual exclusion via the
  `extra` pointer).
* `assign_recovery_target_xid` at `xlogrecovery.c:5035` — sets
  `recoveryTargetXid` and `recoveryTarget = RECOVERY_TARGET_XID`.

#### Comparison logic

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

#### Inclusive vs exclusive

* `recovery_target_inclusive=true` (default): stop **after**
  applying the matching xact record. The transaction's effects
  are visible.
* `recovery_target_inclusive=false`: stop **before** applying.
  The transaction's effects are not visible.

#### Interaction with `recovery_target_timeline`

Independent. The xid match must occur on the WAL stream of the
chosen TLI; if the xid isn't present on that TLI's history, the
recovery exhausts WAL and stops without ever matching.

#### Example

```
recovery_target_xid = '12345'
recovery_target_inclusive = on
```

Recovery replays records up to and including the COMMIT/ABORT for
xid 12345, then `recoveryStopsAfter` returns true. The
`recovery_target_action` then dispatches.

---

---

## 3. `recovery_target_time`

| Field | Value |
|-------|-------|
| Type | string → `TimestampTz` |
| Default | `""` (unset) |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4025` |
| Backing store | `recovery_target_time_string` → `recoveryTargetTime` |

#### Hooks

* `check_recovery_target_time` at `xlogrecovery.c:4895` —
  validates timestamp format. Cannot do final `timestamptz_in`
  parse here because the timezone may not yet be loaded; defers
  to `validateRecoveryParameters`.
* `assign_recovery_target_time` at `xlogrecovery.c:4950`.
* `validateRecoveryParameters` (`xlogrecovery.c:1147-1153`) does
  the final `timestamptz_in`.

#### Comparison

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

#### Inclusive vs exclusive

* `recovery_target_inclusive=true`: stop **after** the last
  transaction whose `xact_time <= recoveryTargetTime`.
* `recovery_target_inclusive=false`: stop **before** the first
  transaction whose `xact_time >= recoveryTargetTime`.

The asymmetry is intentional: many transactions may share the
same commit timestamp (clock granularity), so the inclusive
variant guarantees you see them all, the exclusive variant
guarantees you see none.

#### Example

```
recovery_target_time = '2024-06-15 14:30:00 UTC'
recovery_target_inclusive = on
```

Recovery applies all transactions with `xact_time <=
2024-06-15 14:30:00 UTC` and stops just before the next one.

---

---

## 4. `recovery_target_lsn`

| Field | Value |
|-------|-------|
| Type | string → `XLogRecPtr` (pg_lsn input format) |
| Default | `""` (unset) |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4043` |
| Backing store | `recovery_target_lsn_string` → `recoveryTargetLSN` |

#### Hooks

* `check_recovery_target_lsn` at `xlogrecovery.c:4812` —
  validates with `pg_lsn_in`.
* `assign_recovery_target_lsn` at `xlogrecovery.c:4835`.

#### Comparison

`record->ReadRecPtr` (record start LSN). Stop predicate:

* `recoveryStopsBefore`: `!inclusive &&
   record->ReadRecPtr >= recoveryTargetLSN`.
* `recoveryStopsAfter`: `inclusive &&
   record->ReadRecPtr >= recoveryTargetLSN`.

The LSN check fires on **any record**, not just XACT — so
recovery_target_lsn is the most precise target type.

#### Inclusive vs exclusive

* inclusive=true: stop after the first record whose start LSN is
  ≥ target. The "after" actually means "this record was
  applied".
* inclusive=false: stop before the first record whose start LSN
  is ≥ target.

#### Example

```
recovery_target_lsn = '0/1A234567'
recovery_target_inclusive = off
```

Recovery stops just before applying the first record at LSN
≥ `0/1A234567`. The standby is at `lastReplayedEndRecPtr <
0/1A234567`.

---

---

## 5. `recovery_target_name`

| Field | Value |
|-------|-------|
| Type | string |
| Default | `""` (unset) |
| Allowed length | `≤ MAXFNAMELEN - 1 = 63` chars |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4034` |
| Backing store | `recovery_target_name_string` → `recoveryTargetName` |

#### Hooks

* `check_recovery_target_name` at `xlogrecovery.c:4854` — rejects
  names longer than `MAXFNAMELEN-1`; mutual exclusion check.
* `assign_recovery_target_name` at `xlogrecovery.c:4870` — sets
  `recoveryTargetName` and `recoveryTarget = RECOVERY_TARGET_NAME`.

#### Comparison logic

A `recovery_target_name` only matches `XLOG_RESTORE_POINT` records
created on the primary by `pg_create_restore_point('<name>')`.
The match is at `recoveryStopsAfter` (`xlogrecovery.c:2748`):

```c
if (recoveryTarget == RECOVERY_TARGET_NAME &&
    rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT)
{
    xl_restore_point *recordRestorePointData =
        (xl_restore_point *) XLogRecGetData(record);
    if (strcmp(recordRestorePointData->rp_name, recoveryTargetName) == 0)
    {
        recoveryStopAfter = true;
        ...
        return true;
    }
}
```

#### Inclusive vs exclusive

`recovery_target_inclusive` is **ignored**. Named restore points
are inherently inclusive — the named record is the "moment", and
stopping before it would land at an arbitrary record before the
mark.

#### Interaction with `recovery_target_timeline`

The restore point must lie on the chosen TLI's history. If
multiple restore points share the same name (the function permits
this), recovery stops at the **first** one encountered.

#### Example workflow

1. Operator on primary: `SELECT pg_create_restore_point('preupgrade');`
   ⇒ emits `XLOG_RESTORE_POINT` record with `rp_name="preupgrade"`.
2. Later disaster ⇒ restore base backup, configure
   `recovery_target_name = 'preupgrade'` and start recovery.
3. Recovery replays until the matching record, then dispatches
   `recovery_target_action`.

---

---

## 6. `recovery_target_timeline`

| Field | Value |
|-------|-------|
| Type | string |
| Default | `"latest"` |
| Allowed values | `"latest"`, `"current"`, or a positive decimal/hex TLI |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:3997` |
| Backing store | `recovery_target_timeline_string` → `recoveryTargetTimeLineGoal` (enum) + `recoveryTargetTLIRequested` |

#### Hooks

* `check_recovery_target_timeline` at `xlogrecovery.c:4966` —
  parses to one of three forms:
  * `"latest"` ⇒ `RECOVERY_TARGET_TIMELINE_LATEST`.
  * `"current"` ⇒ `RECOVERY_TARGET_TIMELINE_CONTROLFILE`.
  * numeric ⇒ `RECOVERY_TARGET_TIMELINE_NUMERIC`,
    `recoveryTargetTLIRequested` set.
* `assign_recovery_target_timeline` at `xlogrecovery.c:4999`.

#### Resolution

In `validateRecoveryParameters` (`xlogrecovery.c:1156-1185`):

```c
if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC) {
    if (rtli != 1 && !existsTimeLineHistory(rtli))
        ereport(FATAL, "recovery target timeline %u does not exist", rtli);
    recoveryTargetTLI = rtli;
}
else if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST) {
    recoveryTargetTLI = findNewestTimeLine(recoveryTargetTLI);
}
/* else CONTROLFILE: keep existing recoveryTargetTLI from pg_control */
```

#### Use during recovery

`recoveryTargetTLI` is consulted by:

* `readTimeLineHistory(recoveryTargetTLI)` — populates
  `expectedTLEs` (the list of TLIs we may encounter while
  replaying).
* `tliInHistory(record_tli, expectedTLEs)` in `ReadRecord` —
  validates each page's TLI is on our chosen branch.
* `tliOfPointInHistory(lsn, expectedTLEs)` in
  `WaitForWALToBecomeAvailable` — chooses the right segment file
  to read.

#### Mid-recovery TLI follow

Standby mode periodically calls `rescanLatestTimeLine` to refresh
`expectedTLEs` if the primary's been promoted to a new TLI. This
is gated by `recoveryTargetTimeLineGoal == LATEST`.

#### Example

```
recovery_target_timeline = 'latest'
recovery_target_xid = '12345'
```

`validateRecoveryParameters` resolves `recoveryTargetTLI` to the
highest-numbered TLI in the archive. Recovery then walks WAL on
that branch until xid 12345 commits.

---

---

## 7. `recovery_target_inclusive`

| Field | Value |
|-------|-------|
| Type | bool |
| Default | `true` |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:1767` |
| Backing store | `recoveryTargetInclusive` |

#### Hooks

`NULL` / `NULL` — no custom check or assign. Standard bool GUC.

#### Behavior

Controls whether the stop predicate fires **before**
(`recoveryStopsBefore`) or **after** (`recoveryStopsAfter`)
applying the matching record.

| Target type | inclusive=true (default) | inclusive=false |
|-------------|--------------------------|-----------------|
| XID | Stop after applying COMMIT/ABORT for matching xid (txn visible) | Stop before (txn not visible) |
| TIME | Stop after last txn with `xact_time <= target` | Stop before first txn with `xact_time >= target` |
| LSN | Stop after first record at `start_lsn >= target` | Stop before first record at `start_lsn >= target` |
| NAME | Always inclusive (ignored) | Always inclusive (ignored) |
| IMMEDIATE | Not applicable | Not applicable |

#### Why `XID` semantics differ

For XID, "applying" the COMMIT means the transaction's effects
become visible. A common workflow: PITR to the moment **before** a
known-bad transaction with `recovery_target_xid =
<bad_xid>; recovery_target_inclusive = off`.

---

---

## 8. `recovery_target_action`

| Field | Value |
|-------|-------|
| Type | enum |
| Default | `pause` |
| Allowed values | `pause`, `promote`, `shutdown` |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4924` |
| Backing store | `recoveryTargetAction` (`RecoveryTargetAction` enum in `xlog_internal.h:322`) |

#### Hooks

`NULL` / `NULL` — standard enum GUC.

#### `RecoveryTargetAction` enum

```c
typedef enum
{
    RECOVERY_TARGET_ACTION_PAUSE,
    RECOVERY_TARGET_ACTION_PROMOTE,
    RECOVERY_TARGET_ACTION_SHUTDOWN,
} RecoveryTargetAction;
```

#### Special: PAUSE → SHUTDOWN demotion

`validateRecoveryParameters` (`xlogrecovery.c:1139-1141`) demotes
PAUSE to SHUTDOWN if `hot_standby` is off:

```c
if (recoveryTargetAction == RECOVERY_TARGET_ACTION_PAUSE &&
    !EnableHotStandby)
    recoveryTargetAction = RECOVERY_TARGET_ACTION_SHUTDOWN;
```

Reasoning: pausing requires backends to be able to connect (so
they can call `pg_wal_replay_resume`). Without hot_standby,
no backend can connect; pausing would leave the cluster in a
non-recoverable wedge.

#### Behavior table

| Action | What happens after stop predicate fires |
|--------|----------------------------------------|
| `pause` | `SetRecoveryPause(true)` + `recoveryPausesHere(true)` blocks on a CV. Operator runs `pg_wal_replay_resume()` to promote, or `pg_promote()` directly. |
| `promote` | Falls through to `FinishWalRecovery` → TLI bump → `DB_IN_PRODUCTION`. |
| `shutdown` | `proc_exit(3)` — postmaster sees clean exit; cluster transitions to `DB_SHUTDOWNED_IN_RECOVERY` and stops. Restart will resume. |

#### Source

The dispatch is in `PerformWalRecovery` (`xlogrecovery.c:1851-1869`):

```c
switch (recoveryTargetAction) {
    case RECOVERY_TARGET_ACTION_SHUTDOWN:
        proc_exit(3);
    case RECOVERY_TARGET_ACTION_PAUSE:
        SetRecoveryPause(true);
        recoveryPausesHere(true);
        /* fall through to promote when resumed */
    case RECOVERY_TARGET_ACTION_PROMOTE:
        break;
}
```

The fall-through from PAUSE to PROMOTE is intentional: resume
after pause = promote.

#### Example

```
recovery_target_lsn = '0/1A000000'
recovery_target_action = 'shutdown'
```

Recovery replays up to (and including) the first record at LSN ≥
`0/1A000000`, then `proc_exit(3)`. The cluster is left in
`DB_SHUTDOWNED_IN_RECOVERY` — restartable for further recovery.

---

---


## 9. `recovery_min_apply_delay`

| Field | Value |
|-------|-------|
| Type | int (milliseconds) |
| Default | `0` |
| Range | `0` … `INT_MAX` |
| Context | `PGC_SIGHUP`, `REPLICATION_STANDBY` |
| GUC table entry | `guc_tables.c:2172` |
| Backing store | `recovery_min_apply_delay` |

#### Hooks

`NULL` / `NULL` — standard int GUC.

#### Comparison

The check is in `recoveryApplyDelay` (`xlogrecovery.c:2982`):

```c
if (recovery_min_apply_delay <= 0) return false;
if (!reachedConsistency) return false;
if (!ArchiveRecoveryRequested) return false;
if (XLogRecGetRmid(record) != RM_XACT_ID) return false;
xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
if (xact_info != XLOG_XACT_COMMIT && xact_info != XLOG_XACT_COMMIT_PREPARED)
    return false;
if (!getRecordTimestamp(record, &xtime)) return false;
delayUntil = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay);
/* sleep until delayUntil with periodic interrupt checks */
```

#### Stop predicate

Not a stop — a delay. The redo loop blocks before applying COMMIT
records until `xact_time + recovery_min_apply_delay <= now`.

* Aborts are not delayed (no MVCC effect).
* The clock used is the **primary's** `xact_time`, not the
  standby's reception time. Two standbys with the same delay value
  will reach the same application LSN at the same wall-clock time.

#### Inclusive semantics

Not applicable — this is a delay GUC, not a target.

#### Post-stop action

Not applicable. Promote can interrupt the wait via
`CheckForStandbyTrigger`.

#### Example

```ini
# postgresql.conf
recovery_min_apply_delay = 1h    # apply commits 1 hour late
```

A standby with this setting maintains a one-hour-behind copy of the
primary, useful for accidental DROP TABLE protection: an operator
notices the bad command within an hour and promotes the standby
before it replays the COMMIT.

---


## Cross-references

* For `validateRecoveryParameters`, `recoveryStopsBefore`, `recoveryStopsAfter`, and the post-stop dispatch: [07_recovery_target_system.md](07_recovery_target_system.md).
* For pause/resume mechanics (`recoveryPausesHere`, `pg_wal_replay_pause/resume`): [07_recovery_target_system.md](07_recovery_target_system.md).
* For timeline resolution: [08_timelines.md](08_timelines.md).
* For one-line-per-GUC overview: [appendix_recovery_target_quick_reference.md](appendix_recovery_target_quick_reference.md).
* For all recovery-related GUCs: [appendix_guc_parameters.md](appendix_guc_parameters.md).