# Redo Callbacks: `smgr_redo`, `dbase_redo`, `tblspc_redo`

These three callbacks operate at the *filesystem* layer of the
storage manager: relfilenodes, database directories, tablespace
symlinks.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `smgr_redo` — RM_SMGR_ID = 2

### Identity

* **rmgr id**: `RM_SMGR_ID = 2`
* **rmgr name**: `"Storage"`
* **redo function**: `smgr_redo` at
  `src/backend/catalog/storage.c:965`
* **header**: declared in `src/include/catalog/storage_xlog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x10` | `XLOG_SMGR_CREATE` | Create new relfilenode |
| `0x20` | `XLOG_SMGR_TRUNCATE` | Truncate relation to recorded block count |

Payload structs:

* `xl_smgr_create { RelFileLocator rlocator; ForkNumber forkNum; }`
* `xl_smgr_truncate { RelFileLocator rlocator; BlockNumber blkno; uint32 flags; }`

### State mutations

* Filesystem: `smgrcreate(rel, fork, false)` creates new fork files.
* Filesystem: `smgrtruncate(rel, fork, blkno)` shortens fork files.
* Buffers: `DropRelationsBuffers` evicts pages above truncation
  point.

### Hot-standby behavior

`SMGR_TRUNCATE` indirectly causes snapshot conflicts via the
heap-pruning that accompanies VACUUM TRUNCATE. The smgr record
itself does **not** signal conflicts.

### Idempotency / LSN-skip

* Create is idempotent — `smgrcreate` is a no-op if the file
  already exists.
* Truncate is idempotent — `smgrtruncate` to the same length is a
  no-op.
* Goes through buffer manager (`DropRelationsBuffers`); not via
  `XLogReadBufferForRedo` directly.

### Crash safety

After replay, the on-disk fork files match the post-truncation
size. Subsequent records that refer to truncated blocks will hit
`BLK_NOTFOUND` in `XLogReadBufferForRedo` if the relation is later
dropped, or read the correct (truncated) page otherwise.

### Example

`XLOG_SMGR_TRUNCATE { rel=base/16384/12345, fork=MAIN, blkno=10 }`:

1. `DropRelationsBuffers(rel, fork, blkno)` — invalidates pages
   ≥ 10 in shared buffers.
2. `smgrtruncate(rel, fork, blkno=10)` — `ftruncate` the file.

---

## `dbase_redo` — RM_DBASE_ID = 4

### Identity

* **rmgr id**: `RM_DBASE_ID = 4`
* **rmgr name**: `"Database"`
* **redo function**: `dbase_redo` at
  `src/backend/commands/dbcommands.c:3270`
* **header**: declared in `src/include/commands/dbcommands_xlog.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_DBASE_CREATE_FILE_COPY` | Old-style: copydir(template, new) |
| `0x10` | `XLOG_DBASE_CREATE_WAL_LOG` | New-style: contents replayed via WAL |
| `0x20` | `XLOG_DBASE_DROP` | Drop database |

Payload structs:

* `xl_dbase_create_file_copy_rec`
* `xl_dbase_create_wal_log_rec`
* `xl_dbase_drop_rec`

### State mutations

* Filesystem: `mkdir(base/<dboid>)`, copy template, or `rmtree` on
  drop.
* Buffers: `DropDatabaseBuffers(dbid)` on drop.

### Hot-standby behavior

`XLOG_DBASE_DROP` calls **`ResolveRecoveryConflictWithDatabase`** —
sends `PROCSIG_RECOVERY_CONFLICT_DATABASE` to every backend
connected to the dropped database. Those backends `proc_exit(1)`
(can't recover; the DB is gone). No grace period — the database is
disappearing immediately, not waiting.

### Idempotency / LSN-skip

* CREATE is idempotent if directory already exists.
* DROP is idempotent — `rmtree` of nonexistent path is OK.
* No page-LSN check; operations are at the filesystem level.

### Crash safety

After replay, the database directory either exists (matching the
template, or its WAL-logged content) or has been removed.

### Example

`XLOG_DBASE_DROP { db_id=16384 }`:

1. `ResolveRecoveryConflictWithDatabase(16384)` — kicks all
   backends connected to db 16384 (no wait).
2. `DropDatabaseBuffers(16384)` — evicts all buffers in that DB.
3. `rmtree("base/16384")` — removes data files.

---

## `tblspc_redo` — RM_TBLSPC_ID = 5

### Identity

* **rmgr id**: `RM_TBLSPC_ID = 5`
* **rmgr name**: `"Tablespace"`
* **redo function**: `tblspc_redo` at
  `src/backend/commands/tablespace.c:1511`
* **header**: declared in `src/include/commands/tablespace.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_TBLSPC_CREATE` | Create tablespace + symlink |
| `0x10` | `XLOG_TBLSPC_DROP` | Drop tablespace + symlink |

Payload structs:

* `xl_tblspc_create_rec { Oid ts_id; char ts_path[FLEXIBLE_ARRAY_MEMBER]; }`
* `xl_tblspc_drop_rec { Oid ts_id; }`

### State mutations

* Filesystem: `pg_tblspc/<ts_id>` symlink created/destroyed.
* Filesystem: target directory tree created/destroyed.

### Hot-standby behavior

`XLOG_TBLSPC_DROP` calls
**`ResolveRecoveryConflictWithTablespace(ts_id)`** which uses
`ResolveRecoveryConflictWithVirtualXIDs` (the standard wait-cancel
path) to clear backends with **temp files in the target
tablespace** (`GetConflictingVirtualXIDs(temp_namespace=ts_id)`).
Subject to `max_standby_*_delay`.

### Idempotency / LSN-skip

* CREATE is idempotent — `mkdir` of existing dir is OK; symlink is
  re-created.
* DROP is idempotent — `unlink` + `rmtree` of nonexistent path is OK.

### Crash safety

After replay, `pg_tblspc/<ts_id>` either exists pointing at the
right path or is gone.

### Example

`XLOG_TBLSPC_DROP { ts_id=16400 }`:

1. `ResolveRecoveryConflictWithTablespace(16400)` — wait up to
   `max_standby_*_delay` for backends with temp files in ts 16400
   to release; cancel any that don't.
2. `destroy_tablespace_directories(16400, true)` — recursive
   removal.
3. `unlink("pg_tblspc/16400")` — remove symlink.

---

## Source references

* `src/backend/catalog/storage.c:965` — `smgr_redo`
* `src/backend/commands/dbcommands.c:3270` — `dbase_redo`
* `src/backend/commands/tablespace.c:1511` — `tblspc_redo`
* `src/include/catalog/storage_xlog.h` — smgr structs
* `src/include/commands/dbcommands_xlog.h` — dbase structs
* `src/include/commands/tablespace.h` — tblspc structs
