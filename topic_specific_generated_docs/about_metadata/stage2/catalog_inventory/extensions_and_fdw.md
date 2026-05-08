# Catalog Inventory: Extensions and Foreign Data Wrappers

## pg_extension (3079) — installed extensions

- **Identity**: 3079, `pg_extension.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      extname;
  Oid           extowner;
  Oid           extnamespace;
  bool          extrelocatable;
  text          extversion;
  /* Oid[] extconfig, text[] extcondition */
  ```
- **Indexes**:
  - `pg_extension_oid_index` (3080, unique).
  - `pg_extension_name_index` (3081, unique).
- **Modification API**: `CreateExtension`, `AlterExtensionStmt`,
  `RemoveExtensionById` (`commands/extension.c`).
- **Cache identifier**: `EXTENSIONOID`, `EXTENSIONNAME`.
- **Dependencies**: extowner → pg_authid, extnamespace → pg_namespace.
  Every object created during `CREATE EXTENSION` gets a
  `DEPENDENCY_EXTENSION ('e')` row pointing at the extension.

## pg_foreign_data_wrapper (2328) — FDW handlers

- **Identity**: 2328, `pg_foreign_data_wrapper.h`, no .dat.
- **Schema**:
  ```c
  Oid           oid;
  NameData      fdwname;
  Oid           fdwowner;
  Oid           fdwhandler;
  Oid           fdwvalidator;
  /* aclitem[] fdwacl, text[] fdwoptions */
  ```
- **Indexes**:
  - `pg_foreign_data_wrapper_oid_index` (112, unique).
  - `pg_foreign_data_wrapper_name_index` (548, unique).
- **Modification API**: `CreateForeignDataWrapper`,
  `AlterForeignDataWrapper`, `RemoveForeignDataWrapperById`.
- **Cache identifier**: `FOREIGNDATAWRAPPEROID`, `FOREIGNDATAWRAPPERNAME`.

## pg_foreign_server (1417) — foreign servers

- **Identity**: 1417, `pg_foreign_server.h`, no .dat.
- **Schema**:
  ```c
  Oid           oid;
  NameData      srvname;
  Oid           srvowner;
  Oid           srvfdw;
  /* text srvtype, text srvversion, aclitem[] srvacl, text[] srvoptions */
  ```
- **Indexes**:
  - `pg_foreign_server_oid_index` (113, unique).
  - `pg_foreign_server_name_index` (549, unique).
- **Modification API**: `CreateForeignServer`, `AlterForeignServer`.
- **Cache identifier**: `FOREIGNSERVEROID`, `FOREIGNSERVERNAME`.

## pg_foreign_table (3118) — foreign-table options

- **Identity**: 3118, `pg_foreign_table.h`, no .dat.
- **Schema**:
  ```c
  Oid           ftrelid;
  Oid           ftserver;
  /* text[] ftoptions */
  ```
- **Indexes**: `pg_foreign_table_relid_index` (3119, unique, (ftrelid)).
- **Modification API**: `CreateForeignTable`, `RemoveForeignTableById`.
- **Cache identifier**: `FOREIGNTABLEREL`.
- **Dependencies**: ftrelid → pg_class (DEPENDENCY_INTERNAL).

## pg_user_mapping (1418) — user mappings for foreign servers

- **Identity**: 1418, `pg_user_mapping.h`, no .dat.
- **Schema**:
  ```c
  Oid           oid;
  Oid           umuser;
  Oid           umserver;
  /* text[] umoptions */
  ```
- **Indexes**:
  - `pg_user_mapping_oid_index` (174, unique).
  - `pg_user_mapping_user_server_index` (175, unique, (umuser, umserver)).
- **Modification API**: `CreateUserMapping`, `AlterUserMapping`,
  `RemoveUserMappingById`.
- **Cache identifier**: `USERMAPPINGOID`, `USERMAPPINGUSERSERVER`.

## pg_transform (3576) — datatype transforms for procedural languages

- **Identity**: 3576, `pg_transform.h`, no .dat.
- **Schema**:
  ```c
  Oid           oid;
  Oid           trftype;
  Oid           trflang;
  regproc       trffromsql;
  regproc       trftosql;
  ```
- **Indexes**:
  - `pg_transform_oid_index` (3574, unique).
  - `pg_transform_type_lang_index` (3575, unique, (trftype, trflang)).
- **Cache identifier**: `TRFOID`, `TRFTYPELANG`.

## Cross-references

- `component_catalog_modification_apis.md` — extension installation,
  CREATE FOREIGN TABLE flow.
