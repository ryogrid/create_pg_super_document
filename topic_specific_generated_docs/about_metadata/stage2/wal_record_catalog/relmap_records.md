# WAL Record Catalog: RelMap (RM_RELMAP_ID)

`RM_RELMAP_ID = "RelMap"`, redo: `relmap_redo`
(`src/backend/utils/cache/relmapper.c`).

## XLOG_RELMAP_UPDATE  (info 0x00)

- **Header**: `relmapper.h:25`.
- **Payload**:
  ```c
  /* relmapper.h:27 */
  typedef struct xl_relmap_update
  {
      Oid    dbid;            /* database ID; 0 if shared map */
      Oid    tsid;            /* tablespace OID; pg_global if shared */
      int32  nbytes;          /* size of the embedded RelMapFile */
      char   data[FLEXIBLE_ARRAY_MEMBER];
  } xl_relmap_update;
  ```
  The `data` payload is a complete `RelMapFile` (524 bytes typical):
  magic + count + 64 RelMapping entries + crc.

- **Emitter**: `perform_relmap_update()` (called from
  `AtEOXact_RelationMap(true)` or `RelationMapFinishBootstrap`). Always
  emitted *before* the on-disk rename, so a standby always sees the new
  map even if the primary crashes mid-rename.

- **Redo**: `relmap_redo`:
  1. Validate the embedded RelMapFile (magic + crc).
  2. `write_relmap_file_internal(buffer, dbid, tsid)` — same atomic
     temp+fsync+rename+fsync-parent-dir as the primary.
  3. Update in-memory `shared_map` or `local_map`.
  4. `CacheInvalidateRelmap` so other backends re-read.

- **Makes durable**: a complete new RelMapFile contents (catalog OID →
  relfilenode mapping for nailed/shared catalogs).

- **Full-page image**: not applicable — the entire RelMapFile (≤524 bytes)
  is in the WAL record itself.

- **Standby effects**:
  - File system: `global/pg_filenode.map` or
    `base/<dbid>/pg_filenode.map` is rewritten.
  - In-memory: `shared_map` or `local_map` updated.
  - Caches: every backend on the standby gets a `SHAREDINVALRELMAP_ID`
    sinval message; `RelationMapInvalidate` re-reads the file. Relcache
    entries for the affected catalog have `rd_node` updated on next
    open.

## When is XLOG_RELMAP_UPDATE emitted?

- VACUUM FULL on a mapped catalog: the new file is built, `relfilenode`
  is updated in the relmap, the WAL record carries the new map.
- CLUSTER on a mapped catalog: same.
- REINDEX on a mapped catalog's index: same (the index itself is mapped).
- `RelationMapFinishBootstrap` at initdb time.
- TRUNCATE on a mapped catalog: NOT allowed; tablecmds.c rejects it.

## Why the full file?

The 524-byte file is small; it is cheaper to log the whole file than to log
diffs and risk inconsistent state. Each WAL record is self-describing:
replaying any one XLOG_RELMAP_UPDATE results in a fully-valid map.

## Cross-references

- `component_relmapper.md` — full design and atomic-write protocol.
- `component_catalog_data_model_and_bootstrap.md` — nailed/shared catalogs
  that depend on the relmap.
