# Redo Callbacks: `seq_redo`, `replorigin_redo`, `relmap_redo`, `generic_redo`, `logicalmsg_redo`

The five "miscellaneous" redo callbacks. Each is small; together
they cover sequences, replication origin tracking, the relmap
file, generic page-delta logging (extension surface), and logical
decoding messages.

[Top index for symbol-by-symbol pages](../../README.md)

---

## `seq_redo` — RM_SEQ_ID = 15

* **redo function**: `seq_redo` at
  `src/backend/commands/sequence.c:1834`
* **header**: `src/include/commands/sequence.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_SEQ_LOG` | Copy sequence tuple onto page |

### State mutations

Sequence relation page (a heap with one tuple). The replay
overwrites the page with the recorded tuple+state.

### Hot-standby behavior

None — sequences are not subject to snapshot conflicts (they're
not transactional in the MVCC sense).

### Idempotency / LSN-skip

Goes through `XLogReadBufferForRedo`; LSN-skipped.

---

## `replorigin_redo` — RM_REPLORIGIN_ID = 19

* **redo function**: `replorigin_redo` at
  `src/backend/replication/logical/origin.c:827`
* **header**: `src/include/replication/origin.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_REPLORIGIN_SET` | `replorigin_advance(node, lsn)` |
| `0x10` | `XLOG_REPLORIGIN_DROP` | Remove origin entry |

### State mutations

`pg_replication_origin` progress in shared memory + on-disk
`pg_logical/replorigin_checkpoint`.

### Hot-standby behavior

No direct implications. Matters for cascaded logical
replication: a cascaded standby replays origin advances so it can
honor `replorigin_session_origin` filtering when its own logical
walsender emits changes.

### Idempotency / LSN-skip

Origin progress is LSN-monotone; advancing to an already-passed
LSN is a no-op.

---

## `relmap_redo` — RM_RELMAP_ID = 7

* **redo function**: `relmap_redo` at
  `src/backend/utils/cache/relmapper.c:1096`
* **header**: `src/include/utils/relmapper.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_RELMAP_UPDATE` | Rewrite `pg_filenode.map` |

The relmap is the special pre-bootstrap mapping for shared
catalogs and a few others (see `RelationMapper`); it can't live
in `pg_class` because it must be readable before `pg_class` is
accessible.

### State mutations

`$PGDATA/global/pg_filenode.map` (shared) or
`$PGDATA/base/<dboid>/pg_filenode.map` (per-database).

### Hot-standby behavior

Forces relcache invalidation for mapped relations
(`RelationCacheInvalidate`), so standby backends re-read mapped
relfilenodes after replay.

### Idempotency / LSN-skip

The on-disk file is rewritten atomically (rename of temp file).
Replay is idempotent.

---

## `generic_redo` — RM_GENERIC_ID = 20

* **redo function**: `generic_redo` at
  `src/backend/access/transam/generic_xlog.c:478`
* **header**: `src/include/access/generic_xlog.h`

### Handled records

A single record type that carries a list of page-deltas (start
offset, length, payload bytes) recorded via `generic_xlog.c` API
(`GenericXLogStart`, `GenericXLogRegisterBuffer`, etc.).

### State mutations

Arbitrary buffer pages owned by the extension that emitted the
record.

### Hot-standby behavior

Extension-defined; default safe — generic_xlog records do not
emit conflict signals. Extensions that need to invalidate
snapshots must use a custom rmgr instead.

### Idempotency / LSN-skip

The deltas reference target buffers via the standard block-ref
mechanism, so they go through `XLogReadBufferForRedo` and obey
LSN-skip.

### Use case

Used by extensions that need WAL logging but don't want a custom
rmgr. The classic in-tree user is `bloom` (a `contrib/` index AM)
which uses generic_xlog for its WAL records.

---

## `logicalmsg_redo` — RM_LOGICALMSG_ID = 21

* **redo function**: `logicalmsg_redo` at
  `src/backend/replication/logical/message.c:87`
* **header**: `src/include/replication/message.h`

### Handled records

| Info | Constant | Purpose |
|------|----------|---------|
| `0x00` | `XLOG_LOGICAL_MESSAGE` | No-op in redo (decoding only) |

### State mutations

None on the redo path. The record exists purely for logical
decoding consumers (`pg_logical_emit_message` SQL function).

### Hot-standby behavior

None.

### Idempotency / LSN-skip

Trivially idempotent (no-op).

---

## Source references

* `src/backend/commands/sequence.c:1834` — `seq_redo`
* `src/backend/replication/logical/origin.c:827` — `replorigin_redo`
* `src/backend/utils/cache/relmapper.c:1096` — `relmap_redo`
* `src/backend/access/transam/generic_xlog.c:478` — `generic_redo`
* `src/backend/replication/logical/message.c:87` — `logicalmsg_redo`
