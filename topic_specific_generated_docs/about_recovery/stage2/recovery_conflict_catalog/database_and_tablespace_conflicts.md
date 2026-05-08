# Recovery Conflict Catalog: DATABASE and TABLESPACE

Two conflicts triggered by **filesystem-level drops** during
replay.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `PROCSIG_RECOVERY_CONFLICT_DATABASE`

* **Enum value**: `procsignal.h:42`
* **Conflict type**: a database is being dropped while standby
  backends are connected to it.

### Triggering event

`dbase_redo XLOG_DBASE_DROP` — the database directory is about to
be `rmtree`'d.

### Resolver

* `ResolveRecoveryConflictWithDatabase`
  (`src/backend/storage/ipc/standby.c:568`).
* **No grace period** — DB is gone now, not later.

### Grace-period GUC

None — no waiting at all.

### Victim selection

`CountDBBackends(dbid)` walks the procarray. Every backend with
`MyDatabaseId == dbid` is signaled via
`CancelDBBackends(dbid, PROCSIG_RECOVERY_CONFLICT_DATABASE,
/*conflictPending=*/true)`.

### Backend response

`ProcessRecoveryConflictInterrupt(reason=DATABASE)` ⇒
`proc_exit(1)`. The backend cannot recover — its database is
gone. There's no point in `ereport(ERROR)` since the next
operation would fail at the filesystem layer anyway.

### Logging

```
FATAL:  terminating connection due to conflict with recovery
DETAIL:  User was connected to a database that must be dropped.
```

### Mitigation

Don't drop databases on the primary while standby backends are
connected. (This is a transient operational issue — the standby
catches up, the database is gone, new connections succeed against
remaining databases.)

### Example scenario

Primary executes `DROP DATABASE devdb`. Two backends are
connected to devdb on the standby. When `dbase_redo` runs:

1. `ResolveRecoveryConflictWithDatabase(devdb_oid)`:
   * Walk procarray; find the two backends.
   * `SendProcSignal(reason=DATABASE)` to each.
   * No wait — return immediately.
2. `DropDatabaseBuffers(devdb_oid)`.
3. `rmtree("base/devdb_oid")`.
4. The two backends, on next CFI, `proc_exit(1)`.

---

## `PROCSIG_RECOVERY_CONFLICT_TABLESPACE`

* **Enum value**: `procsignal.h:43`
* **Conflict type**: a tablespace is being dropped that contains
  in-use temp files (or other in-use files) belonging to standby
  backends.

### Triggering event

`tblspc_redo XLOG_TBLSPC_DROP`.

### Resolver

* `ResolveRecoveryConflictWithTablespace`
  (`src/backend/storage/ipc/standby.c:538`).
* Dispatches to `ResolveRecoveryConflictWithVirtualXIDs`.

### Grace-period GUC

* `max_standby_archive_delay` / `max_standby_streaming_delay`.

### Victim selection

`GetConflictingVirtualXIDs(InvalidTransactionId, InvalidOid)` for
backends with **temp files** in the target tablespace. The temp
namespace is per-backend; the procarray entries that reference
`temp_tablespaces` containing the target are flagged.

### Backend response

`ProcessRecoveryConflictInterrupt(reason=TABLESPACE)` ⇒
`ereport(ERROR, "canceling statement due to conflict with
recovery", DETAIL "User was using a tablespace that must be
dropped.")`.

### Logging

When `log_recovery_conflict_waits=on`,
`LogRecoveryConflict(reason=TABLESPACE)`.

### Mitigation

* Coordinate tablespace drops with standby workload.
* Increase `max_standby_*_delay`.

### Example scenario

Primary executes `DROP TABLESPACE testts`. Standby backend has a
temp table in `testts`. When `tblspc_redo` runs:

1. `ResolveRecoveryConflictWithTablespace(testts_oid)`:
   * Walk procarray for backends with temp files in testts_oid.
   * For each: `SendProcSignal(reason=TABLESPACE)`.
   * Wait up to `max_standby_streaming_delay`.
2. After timeout (or earlier release): backends are canceled.
3. `destroy_tablespace_directories(testts_oid, true)`.
4. `unlink("pg_tblspc/testts_oid")`.

---

## Source references

* `src/include/storage/procsignal.h:42-43` —
  `PROCSIG_RECOVERY_CONFLICT_{DATABASE,TABLESPACE}`
* `src/backend/storage/ipc/standby.c:538` —
  `ResolveRecoveryConflictWithTablespace`
* `src/backend/storage/ipc/standby.c:568` —
  `ResolveRecoveryConflictWithDatabase`
* `src/backend/storage/ipc/procarray.c` — `CountDBBackends`,
  `CancelDBBackends`, `GetConflictingVirtualXIDs`
* `src/backend/commands/dbcommands.c:3270` — `dbase_redo`
* `src/backend/commands/tablespace.c:1511` — `tblspc_redo`
