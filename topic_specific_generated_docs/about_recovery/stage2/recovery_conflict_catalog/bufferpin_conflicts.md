# Recovery Conflict Catalog: BUFFERPIN

The conflict triggered when the **startup process needs cleanup
access to a buffer** that a standby backend has pinned.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `PROCSIG_RECOVERY_CONFLICT_BUFFERPIN`

* **Enum value**: `procsignal.h:47`
* **Conflict type**: startup process needs to acquire
  `LockBufferForCleanup` (an exclusive pin) on a shared buffer
  another backend has pinned.

### Triggering event

Any redo callback that calls `LockBufferForCleanup` while the
buffer is pinned by another backend. In practice:

* `heap2_redo XLOG_HEAP2_VISIBLE` — setting the VM all-visible bit
  requires a cleanup lock on the heap page.
* `heap2_redo XLOG_HEAP2_PRUNE_*` — pruning needs cleanup lock.
* `btree_redo`, `hash_redo` VACUUM-class records.

### Resolver

* `ResolveRecoveryConflictWithBufferPin`
  (`src/backend/storage/ipc/standby.c:792`).
* Different from the others: instead of building a VXID list and
  calling `ResolveRecoveryConflictWithVirtualXIDs`, it sets a
  `STANDBY_TIMEOUT` alarm, then signals **every** active backend
  via `SendRecoveryConflictWithBufferPin`. Backends that don't
  hold the relevant pin ignore the signal (filtered via
  `RecoveryConflictPendingReasons[]`).

### Grace-period GUC

* `max_standby_archive_delay` / `max_standby_streaming_delay`
  — used as the `STANDBY_TIMEOUT` seed.

### Victim selection

The signal is broadcast to all backends; only those actually
holding pins on the targeted buffer set
`RecoveryConflictPending`. The others simply observe
`RecoveryConflictPendingReasons[BUFFERPIN] == false` (no pending
work for them) at next CFI.

### Backend response

`ProcessRecoveryConflictInterrupt(reason=BUFFERPIN)`:

* If the backend is **idle** (no statement in progress) AND is the
  one blocking startup ⇒ release the buffer pin without canceling
  (special path — pin can be released without aborting the
  transaction).
* Else ⇒ `ereport(ERROR, "canceling statement due to conflict
  with recovery")`.

The "release pin if idle" path is a real performance optimization:
many idle psql sessions sit on buffers via cursors; canceling them
would be heavy-handed when the pin can simply be dropped.

### Logging

When `log_recovery_conflict_waits=on`,
`LogRecoveryConflict(reason=BUFFERPIN)`.

### Mitigation

* Avoid long-held cursors on a standby (their pins can block
  vacuum-related redo).
* Increase `max_standby_*_delay`.

### Example scenario

A backend on the standby is reading a large table via a holdable
cursor (which keeps a buffer pin between fetches). The primary
runs VACUUM and emits `XLOG_HEAP2_VISIBLE` to set the VM bit on
that page. When `heap2_redo` runs:

1. `XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL,
   /*get_cleanup_lock=*/true, &buf)` — needs a *cleanup* lock.
2. `LockBufferForCleanup` returns: buffer is pinned by another
   backend.
3. Set `STANDBY_TIMEOUT = max_standby_streaming_delay`.
4. `SendRecoveryConflictWithBufferPin` — broadcast signal.
5. Wait on a sleep loop polling `LockBufferForCleanup`.
6. If the cursor backend is idle: it releases its pin via the
   special-case path, startup acquires the cleanup lock, applies
   the VM update.
7. Else, after STANDBY_TIMEOUT: backend cancelled, pin released,
   startup proceeds.

---

## Source references

* `src/include/storage/procsignal.h:47` —
  `PROCSIG_RECOVERY_CONFLICT_BUFFERPIN`
* `src/backend/storage/ipc/standby.c:792` —
  `ResolveRecoveryConflictWithBufferPin`
* `src/backend/storage/ipc/standby.c` —
  `SendRecoveryConflictWithBufferPin`
* `src/backend/storage/buffer/bufmgr.c` — `LockBufferForCleanup`
* `src/backend/tcop/postgres.c` — bufferpin idle-release special
  case
