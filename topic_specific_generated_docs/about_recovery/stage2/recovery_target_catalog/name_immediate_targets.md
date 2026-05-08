# Recovery Target Catalog: `recovery_target_name` and `recovery_target = 'immediate'`

The two **non-numeric** target types: a named restore point and
the special "as soon as consistent" target.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `recovery_target_name`

| Field | Value |
|-------|-------|
| Type | string |
| Default | `""` (unset) |
| Allowed length | `≤ MAXFNAMELEN - 1 = 63` chars |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4034` |
| Backing store | `recovery_target_name_string` → `recoveryTargetName` |

### Hooks

* `check_recovery_target_name` at `xlogrecovery.c:4854` — rejects
  names longer than `MAXFNAMELEN-1`; mutual exclusion check.
* `assign_recovery_target_name` at `xlogrecovery.c:4870` — sets
  `recoveryTargetName` and `recoveryTarget = RECOVERY_TARGET_NAME`.

### Comparison logic

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

### Inclusive vs exclusive

`recovery_target_inclusive` is **ignored**. Named restore points
are inherently inclusive — the named record is the "moment", and
stopping before it would land at an arbitrary record before the
mark.

### Interaction with `recovery_target_timeline`

The restore point must lie on the chosen TLI's history. If
multiple restore points share the same name (the function permits
this), recovery stops at the **first** one encountered.

### Example workflow

1. Operator on primary: `SELECT pg_create_restore_point('preupgrade');`
   ⇒ emits `XLOG_RESTORE_POINT` record with `rp_name="preupgrade"`.
2. Later disaster ⇒ restore base backup, configure
   `recovery_target_name = 'preupgrade'` and start recovery.
3. Recovery replays until the matching record, then dispatches
   `recovery_target_action`.

---

## `recovery_target = 'immediate'`

| Field | Value |
|-------|-------|
| Type | string |
| Default | `""` |
| Allowed values | `""` (unset), `"immediate"` |
| Context | `PGC_POSTMASTER` |
| GUC table entry | `guc_tables.c:4007` |
| Backing store | `recovery_target_string` → `recoveryTarget` enum |

### Hooks

* `check_recovery_target` at `xlogrecovery.c:4782` — only accepts
  `""` or `"immediate"`; mutual exclusion check.
* `assign_recovery_target` at `xlogrecovery.c:4796` — sets
  `recoveryTarget = RECOVERY_TARGET_IMMEDIATE`.

### Comparison logic

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

### Use case

The minimum recovery — replay only as much as needed to make the
on-disk state consistent. Used for:

* Restoring a base backup quickly to an ad-hoc state.
* Diagnostic recoveries (don't replay further than the data
  itself requires).

### Inclusive vs exclusive

Not applicable. The stop is "as soon as consistent" — there's no
inclusive/exclusive choice.

### Example

```
recovery_target = 'immediate'
recovery_target_action = 'shutdown'
```

The cluster recovers the backup, reaches consistency, then exits
cleanly leaving `pg_control` in `DB_SHUTDOWNED_IN_RECOVERY`.

---

## `RecoveryTargetType` enum (`src/include/access/xlogrecovery.h:23`)

```c
typedef enum
{
    RECOVERY_TARGET_UNSET,
    RECOVERY_TARGET_XID,
    RECOVERY_TARGET_TIME,
    RECOVERY_TARGET_NAME,
    RECOVERY_TARGET_LSN,
    RECOVERY_TARGET_IMMEDIATE,
} RecoveryTargetType;
```

Exactly one of these may be set at a time; mutual exclusion is
enforced by the per-GUC `check_*` hooks via the `extra` slot.

---

## Source references

* `src/backend/access/transam/xlogrecovery.c:4782-4796` —
  `recovery_target` hooks (immediate)
* `src/backend/access/transam/xlogrecovery.c:4854-4870` —
  `recovery_target_name` hooks
* `src/backend/access/transam/xlogrecovery.c:2589` — IMMEDIATE
  stop predicate
* `src/backend/access/transam/xlogrecovery.c:2747-2768` — NAME
  stop predicate (in `recoveryStopsAfter`)
* `src/include/access/xlog.h` — `xl_restore_point` struct
* `src/include/access/xlogrecovery.h:23` — `RecoveryTargetType`
  enum
* `src/backend/utils/misc/guc_tables.c:4007, :4034` — GUC table
  entries
