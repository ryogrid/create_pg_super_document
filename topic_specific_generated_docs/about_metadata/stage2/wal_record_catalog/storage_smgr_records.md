# WAL Record Catalog: Storage Manager (RM_SMGR_ID)

`RM_SMGR_ID = "Storage"`, redo: `smgr_redo`
(`src/backend/catalog/storage.c:965`).

## XLOG_SMGR_CREATE  (info 0x10)

- **Header**: `storage_xlog.h:30`.
- **Payload**:
  ```c
  typedef struct xl_smgr_create
  {
      RelFileLocator rlocator;     /* (spcOid, dbOid, relNumber) */
      ForkNumber     forkNum;      /* MAIN_FORKNUM, INIT_FORKNUM, etc. */
  } xl_smgr_create;
  ```

- **Emitter**: `log_smgrcreate()` (storage.c:186), called from
  `RelationCreateStorage` for permanent relations (skipped for unlogged
  and temp).

- **Redo**: `smgr_redo`:
  1. `smgropen(xlrec->rlocator, INVALID_PROC_NUMBER)`.
  2. `smgrcreate(srel, xlrec->forkNum, true /* is_redo */)` — actually
     creates the file. `is_redo = true` makes smgrcreate tolerant of the
     file already existing.
  3. If forkNum == MAIN_FORKNUM, the FSM and VM forks may also need to be
     ready for subsequent records.

- **Makes durable**: physical creation of one fork of one relfilenode.

- **Full-page image**: not applicable — smgrcreate produces a 0-byte file.

- **Standby effects**: a new file appears in `base/<dbid>/<relfilenode>`
  (or with the fork suffix `_fsm`, `_vm`, `_init`).

- **XLR_SPECIAL_REL_UPDATE**: the record carries this flag, indicating it
  affects relation storage outside of normal heap pages — used by
  pg_rewind / pg_basebackup to know which files to track.

## XLOG_SMGR_TRUNCATE  (info 0x20)

- **Header**: `storage_xlog.h:31`.
- **Payload**:
  ```c
  typedef struct xl_smgr_truncate
  {
      BlockNumber    blkno;       /* heap fork's new block count */
      RelFileLocator rlocator;
      uint32         flags;        /* SMGR_TRUNCATE_HEAP / FSM / VM / ALL */
  } xl_smgr_truncate;
  ```

- **Emitter**: `RelationTruncate()` (storage.c) — invoked by TRUNCATE,
  by VACUUM when shrinking a relation, and by ALTER SEQUENCE.

- **Redo**: `smgr_redo`:
  1. `smgrtruncate(srel, MAIN_FORKNUM, blkno)` if SMGR_TRUNCATE_HEAP.
  2. `visibilitymap_prepare_truncate(rel, blkno)` if SMGR_TRUNCATE_VM —
     truncates the VM fork.
  3. `FreeSpaceMapPrepareTruncateRel(rel, blkno)` if SMGR_TRUNCATE_FSM —
     truncates the FSM fork.

- **Makes durable**: physical truncation of a relation's main fork plus
  in-lockstep truncation of its VM and FSM forks.

- **Full-page image**: not applicable.

- **Standby effects**: relation files shrink; VM and FSM forks shrink
  proportionally.

## Pending-delete bookkeeping

The storage.c module also maintains an in-memory `pendingDeletes` list,
not directly WAL-logged. Entries are added by `RelationCreateStorage`
(`atCommit = false` — undo create on abort) and `RelationDropStorage`
(`atCommit = true` — execute unlink at commit).

The list is consulted by:
- `smgrDoPendingDeletes(true)` at commit — unlinks files marked atCommit.
- `smgrDoPendingDeletes(false)` at abort — unlinks files marked
  !atCommit (the ones we just created).
- `xact_redo_commit` — replays from `xl_xact_relfilelocators` in
  `xl_xact_commit`.

This is why XLOG_XACT_COMMIT carries a list of dropped relfilelocators:
the standby needs to know which files to unlink at commit replay.

## Cross-references

- `component_catalog_modification_apis.md` — RelationCreateStorage,
  RelationDropStorage, smgrDoPendingDeletes.
- `component_persistence_and_wal_records.md` — XACT records carry
  the pending-delete list.
