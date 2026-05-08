# WAL Record Catalog: Database and Tablespace Records

## RM_DBASE_ID — Database

`RM_DBASE_ID = "Database"`, redo: `dbase_redo`
(`src/backend/commands/dbcommands.c`).

### XLOG_DBASE_CREATE_FILE_COPY  (info 0x00)

- **Header**: `dbcommands_xlog.h:21`.
- **Payload**:
  ```c
  typedef struct xl_dbase_create_file_copy_rec
  {
      Oid db_id;
      Oid tablespace_id;
      Oid src_db_id;
      Oid src_tablespace_id;
  } xl_dbase_create_file_copy_rec;
  ```
- **Emitter**: `createdb_failure_callback` path with
  `STRATEGY = FILE_COPY`, the legacy `CREATE DATABASE` strategy.
- **Redo**: copies the source database's directory tree to the new
  location.
- **Makes durable**: CREATE DATABASE via filesystem copy.

### XLOG_DBASE_CREATE_WAL_LOG  (info 0x10)

- **Header**: `dbcommands_xlog.h:22`.
- **Payload**:
  ```c
  typedef struct xl_dbase_create_wal_log_rec
  {
      Oid db_id;
      Oid tablespace_id;
  } xl_dbase_create_wal_log_rec;
  ```
- **Emitter**: `createdb_failure_callback` with `STRATEGY = WAL_LOG`
  (the modern default).
- **Redo**: creates the new database directory; subsequent per-relation
  WAL records replicate the actual page contents.
- **Makes durable**: the new database directory's existence; the contents
  come via subsequent records.

### XLOG_DBASE_DROP  (info 0x20)

- **Header**: `dbcommands_xlog.h:23`.
- **Payload**:
  ```c
  typedef struct xl_dbase_drop_rec
  {
      Oid db_id;
      int ntablespaces;
      Oid tablespace_ids[FLEXIBLE_ARRAY_MEMBER];
  } xl_dbase_drop_rec;
  ```
- **Emitter**: `dropdb`.
- **Redo**: walks `tablespace_ids[]`; for each, removes the
  `<tablespace>/<dbid>` directory recursively.
- **Makes durable**: DROP DATABASE — recursive directory removal across
  every tablespace the database had files in.

## RM_TBLSPC_ID — Tablespace

`RM_TBLSPC_ID = "Tablespace"`, redo: `tblspc_redo`
(`src/backend/commands/tablespace.c`).

### XLOG_TBLSPC_CREATE  (info 0x00)

- **Header**: `tablespace.h:25`.
- **Payload**:
  ```c
  typedef struct xl_tblspc_create_rec
  {
      Oid    ts_id;
      char   ts_path[FLEXIBLE_ARRAY_MEMBER];
  } xl_tblspc_create_rec;
  ```
- **Emitter**: `CreateTableSpace`.
- **Redo**: creates the symlink under `pg_tblspc/<ts_id>` pointing at
  `ts_path`. Creates the per-database subdirectories that already existed
  on the primary (the symlink target on the standby must be writable).
- **Makes durable**: tablespace symlink creation.

### XLOG_TBLSPC_DROP  (info 0x10)

- **Header**: `tablespace.h:26`.
- **Payload**:
  ```c
  typedef struct xl_tblspc_drop_rec
  {
      Oid ts_id;
  } xl_tblspc_drop_rec;
  ```
- **Emitter**: `DropTableSpace`.
- **Redo**: removes the `pg_tblspc/<ts_id>` symlink. The standby is
  expected to have already removed any per-database directories under
  the target via DROP DATABASE replays.
- **Makes durable**: tablespace removal.

## Implicit catalog effects

These records do NOT go through pg_catalog tables on the standby —
they directly manipulate filesystem state. The pg_database / pg_tablespace
catalog rows are written via ordinary heap WAL on the primary, and the
standby applies those via heap_xlog_insert / heap_xlog_delete in the
normal way. The XLOG_DBASE_* / XLOG_TBLSPC_* records are *additional*
records that handle the filesystem side (which heap WAL alone cannot
express, since these are directory-level operations).

## Cross-references

- `component_persistence_and_wal_records.md` — overview of all metadata
  records.
- `catalog_inventory/core_relations.md` — pg_database, pg_tablespace.
