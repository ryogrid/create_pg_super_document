# Catalog Inventory: Miscellaneous

Catalogs that don't fit cleanly into other groupings: large objects,
descriptions, GUC defaults, sequences.

## pg_largeobject (2613) — large object data chunks

- **Identity**: 2613, `pg_largeobject.h`, no .dat, `pg_largeobject.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           loid;
  int32         pageno;
  bytea         data;
  ```
- **Indexes**: `pg_largeobject_loid_pn_index` (2683, unique, (loid, pageno)).
- **Modification API**: `lo_create`, `lo_unlink`, `inv_open`, `inv_read`,
  `inv_write`, `inv_seek`, `inv_close` (large_object.c).
- **Cache identifier**: none (data chunks are not catcached).
- **Dependencies**: implicit via pg_largeobject_metadata.

## pg_largeobject_metadata (2995) — large object owner + ACL

- **Identity**: 2995, `pg_largeobject_metadata.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           lomowner;
  /* aclitem[] lomacl */
  ```
- **Indexes**: `pg_largeobject_metadata_oid_index` (2996, unique).
- **Modification API**: `LargeObjectCreate`, `LargeObjectExists`,
  `lo_drop`, `LargeObjectAlterOwner`.
- **Cache identifier**: `LARGEOBJECTOID`.
- **Dependencies**: lomowner → pg_authid.

## pg_db_role_setting (2964) — ALTER DATABASE/ROLE SET defaults

- **Identity**: 2964, shared, mapped, `pg_db_role_setting.c`.
- **Schema**:
  ```c
  Oid           setdatabase;
  Oid           setrole;
  /* text[] setconfig */
  ```
- **Indexes**: `pg_db_role_setting_databaseid_rol_index` (2965, unique,
  (setdatabase, setrole)).
- **Modification API**: `AlterSetting`, `DropSetting` (`pg_db_role_setting.c`).
- **Cache identifier**: `DATABASEROLE`.

## pg_description (2609) — COMMENTs on non-shared objects

- **Identity**: 2609, `pg_description.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  int32         objsubid;
  text          description;
  ```
- **Indexes**: `pg_description_o_c_o_index` (2675, unique,
  (objoid, classoid, objsubid)).
- **Modification API**: `CreateComments`, `DeleteComments` (`comment.c`).

## pg_shdescription (2396) — COMMENTs on shared objects

- **Identity**: 2396, shared, mapped.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  text          description;
  ```
- **Indexes**: `pg_shdescription_o_c_index` (2397, unique).
- **Modification API**: `CreateSharedComments`, `DeleteSharedComments`.

## pg_sequence (2224) — sequence metadata

- **Identity**: 2224, `pg_sequence.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           seqrelid;
  Oid           seqtypid;
  int64         seqstart;
  int64         seqincrement;
  int64         seqmax;
  int64         seqmin;
  int64         seqcache;
  bool          seqcycle;
  ```
- **Indexes**: `pg_sequence_seqrelid_index` (5002, unique, (seqrelid)).
- **Modification API**: `DefineSequence`, `AlterSequence`,
  `RemoveSequenceById`.
- **Cache identifier**: `SEQRELID`.
- **Dependencies**: seqrelid → pg_class (DEPENDENCY_INTERNAL).

(The current sequence value lives in the relation file itself, not in
pg_sequence; pg_sequence carries only metadata.)

## Cross-references

- `component_catalog_modification_apis.md` — sequence DDL.
- `catalog_inventory/core_relations.md` — pg_class with relkind = 'S'.
