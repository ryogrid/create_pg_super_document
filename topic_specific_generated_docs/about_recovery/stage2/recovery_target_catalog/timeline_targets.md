# Recovery Target Catalog: `recovery_target_timeline`, `recovery_target_inclusive`, `recovery_target_action`

The three "control" GUCs that shape *how* a recovery target is
applied: which timeline branch to follow, whether the matching
record is replayed, and what to do once the target is reached.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `recovery_target_timeline`

| Field | Value |
|-------|-------|
| Type | string |
| Default | `"latest"` |
| Allowed values | `"latest"`, `"current"`, or a positive decimal/hex TLI |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:3997` |
| Backing store | `recovery_target_timeline_string` → `recoveryTargetTimeLineGoal` (enum) + `recoveryTargetTLIRequested` |

### Hooks

* `check_recovery_target_timeline` at `xlogrecovery.c:4966` —
  parses to one of three forms:
  * `"latest"` ⇒ `RECOVERY_TARGET_TIMELINE_LATEST`.
  * `"current"` ⇒ `RECOVERY_TARGET_TIMELINE_CONTROLFILE`.
  * numeric ⇒ `RECOVERY_TARGET_TIMELINE_NUMERIC`,
    `recoveryTargetTLIRequested` set.
* `assign_recovery_target_timeline` at `xlogrecovery.c:4999`.

### Resolution

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

### Use during recovery

`recoveryTargetTLI` is consulted by:

* `readTimeLineHistory(recoveryTargetTLI)` — populates
  `expectedTLEs` (the list of TLIs we may encounter while
  replaying).
* `tliInHistory(record_tli, expectedTLEs)` in `ReadRecord` —
  validates each page's TLI is on our chosen branch.
* `tliOfPointInHistory(lsn, expectedTLEs)` in
  `WaitForWALToBecomeAvailable` — chooses the right segment file
  to read.

### Mid-recovery TLI follow

Standby mode periodically calls `rescanLatestTimeLine` to refresh
`expectedTLEs` if the primary's been promoted to a new TLI. This
is gated by `recoveryTargetTimeLineGoal == LATEST`.

### Example

```
recovery_target_timeline = 'latest'
recovery_target_xid = '12345'
```

`validateRecoveryParameters` resolves `recoveryTargetTLI` to the
highest-numbered TLI in the archive. Recovery then walks WAL on
that branch until xid 12345 commits.

---

## `recovery_target_inclusive`

| Field | Value |
|-------|-------|
| Type | bool |
| Default | `true` |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:1767` |
| Backing store | `recoveryTargetInclusive` |

### Hooks

`NULL` / `NULL` — no custom check or assign. Standard bool GUC.

### Behavior

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

### Why `XID` semantics differ

For XID, "applying" the COMMIT means the transaction's effects
become visible. A common workflow: PITR to the moment **before** a
known-bad transaction with `recovery_target_xid =
<bad_xid>; recovery_target_inclusive = off`.

---

## `recovery_target_action`

| Field | Value |
|-------|-------|
| Type | enum |
| Default | `pause` |
| Allowed values | `pause`, `promote`, `shutdown` |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4924` |
| Backing store | `recoveryTargetAction` (`RecoveryTargetAction` enum in `xlog_internal.h:322`) |

### Hooks

`NULL` / `NULL` — standard enum GUC.

### `RecoveryTargetAction` enum

```c
typedef enum
{
    RECOVERY_TARGET_ACTION_PAUSE,
    RECOVERY_TARGET_ACTION_PROMOTE,
    RECOVERY_TARGET_ACTION_SHUTDOWN,
} RecoveryTargetAction;
```

### Special: PAUSE → SHUTDOWN demotion

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

### Behavior table

| Action | What happens after stop predicate fires |
|--------|----------------------------------------|
| `pause` | `SetRecoveryPause(true)` + `recoveryPausesHere(true)` blocks on a CV. Operator runs `pg_wal_replay_resume()` to promote, or `pg_promote()` directly. |
| `promote` | Falls through to `FinishWalRecovery` → TLI bump → `DB_IN_PRODUCTION`. |
| `shutdown` | `proc_exit(3)` — postmaster sees clean exit; cluster transitions to `DB_SHUTDOWNED_IN_RECOVERY` and stops. Restart will resume. |

### Source

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

### Example

```
recovery_target_lsn = '0/1A000000'
recovery_target_action = 'shutdown'
```

Recovery replays up to (and including) the first record at LSN ≥
`0/1A000000`, then `proc_exit(3)`. The cluster is left in
`DB_SHUTDOWNED_IN_RECOVERY` — restartable for further recovery.

---

## Source references

* `src/backend/access/transam/xlogrecovery.c:4966-4999` —
  `recovery_target_timeline` hooks
* `src/backend/access/transam/xlogrecovery.c:1156-1185` —
  TLI resolution
* `src/backend/access/transam/xlogrecovery.c:1851-1869` —
  recovery_target_action dispatch
* `src/backend/access/transam/xlogrecovery.c:1139-1141` —
  PAUSE→SHUTDOWN demotion
* `src/backend/utils/misc/guc_tables.c:1767, :3997, :4924` — GUC
  table entries
* `src/include/access/xlogrecovery.h` — `RecoveryTargetTimeLineGoal`
* `src/include/access/xlog_internal.h:322` — `RecoveryTargetAction`
