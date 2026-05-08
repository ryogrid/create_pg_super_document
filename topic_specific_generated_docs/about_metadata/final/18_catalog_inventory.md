# 18 — System Catalog Inventory

[Up: index.md](index.md)  |  [Prev: 17 Hooks and Extensibility](17_hooks_and_extensibility.md)  |  [Next: 19 SLRU Users Catalog](19_slru_users_catalog.md)

## Prerequisites

- [03 Catalog Data Model](03_catalog_data_model_and_bootstrap.md) — what nailed/shared/mapped means.
- [04 Catalog Modification APIs](04_catalog_modification_apis.md) — the entry points referenced by every catalog's *Modification API* line.
- [05 Catalog Caches](05_catalog_caches.md) — the syscache identifiers referenced by every catalog's *Cache identifier* line.

This chapter is a catalog-by-catalog reference for every relation in
`pg_catalog`. Each entry gives:

- **Identity**: name, OID, header file, .dat file (if any), C-side helper.
- **Storage flags**: shared / nailed / mapped / local.
- **Schema**: the `FormData_<name>` C struct fields.
- **Indexes**: every `DECLARE_*INDEX*` from the header.
- **Modification API**: the C entry point(s) that mutate this catalog,
  always funneling through the `CatalogTuple{Insert,Update,Delete}`
  trio in [04 Catalog Modification APIs](04_catalog_modification_apis.md).
- **Cache identifier**: the `SysCacheIdentifier` enum entry (chapter
  [05](05_catalog_caches.md)).
- **Dependencies**: what `recordDependencyOn` rows are typically inserted.
- **Bootstrap status**: whether `.dat` rows ship for it.
- **SQL example**: a query you can run against the catalog, or a
  DDL statement that creates rows in it.

Total: 63 catalog tables. Quick-reference table is in
[appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md).

## Section Core relations

Catalogs that describe relations themselves and the cluster-wide scaffolding.

### pg_class (1259) — the relation table

- **Identity**: pg_class, OID 1259, header `src/include/catalog/pg_class.h`,
  bootstrap data `pg_class.dat`.
- **Storage flags**: nailed (`BKI_BOOTSTRAP`), mapped (relfilenode in
  base/<dbid>/pg_filenode.map). Per-database (not shared).
- **Schema** (FormData_pg_class — abridged):
  ```c
  typedef FormData_pg_class {
      Oid          oid;
      NameData     relname;
      Oid          relnamespace;
      Oid          reltype;
      Oid          reloftype;
      Oid          relowner;
      Oid          relam;
      Oid          relfilenode;             /* 0 for mapped relations */
      Oid          reltablespace;
      int32        relpages;
      float4       reltuples;
      int32        relallvisible;
      Oid          reltoastrelid;
      bool         relhasindex;
      bool         relisshared;
      char         relpersistence;          /* 'p' / 't' / 'u' */
      char         relkind;                 /* 'r' / 'i' / 'S' / 't' / 'm' / ... */
      int16        relnatts;
      int16        relchecks;
      bool         relhasrules;
      bool         relhastriggers;
      bool         relhassubclass;
      bool         relrowsecurity;
      bool         relforcerowsecurity;
      bool         relispopulated;
      char         relreplident;
      bool         relispartition;
      Oid          relrewrite;
      TransactionId relfrozenxid;
      MultiXactId  relminmxid;
      /* aclitem[] relacl, text[] reloptions, pg_node_tree relpartbound  */
  };
  ```
- **Indexes**:
  - `pg_class_oid_index` (2662): unique, btree(oid). C macro: `ClassOidIndexId`.
  - `pg_class_relname_nsp_index` (2663): unique, btree(relname, relnamespace).
  - `pg_class_tblspc_relfilenode_index` (3455): non-unique,
    btree(reltablespace, relfilenode). For relfilenode reverse mapping.
- **Modification API**:
  - `heap_create_with_catalog`, `heap_drop_with_catalog` (heap.c)
  - `index_create`, `index_drop` (index.c)
  - `InsertPgClassTuple`, `RelationSetNewRelfilenumber`
  - `heap_inplace_update_and_unlock` for `relfrozenxid`, `relpages`, `reltuples`.
- **Cache identifier**: `RELOID`, `RELNAMENSP`.
- **Dependencies**: row-type → pg_type (DEPENDENCY_INTERNAL), namespace →
  pg_namespace, owner → pg_authid (shared), tablespace → pg_tablespace
  (shared), AM → pg_am.
- **Bootstrap status**: yes; pg_class.dat carries the seed rows for the
  catalog system itself.

### pg_attribute (1249) — column metadata

- **Identity**: 1249, header `pg_attribute.h`, no .dat file.
- **Storage flags**: nailed, mapped.
- **Schema** (FormData_pg_attribute):
  ```c
  Oid          attrelid;
  NameData     attname;
  Oid          atttypid;
  int16        attlen;
  int16        attnum;
  int32        attndims;
  int32        attcacheoff;
  int32        atttypmod;
  bool         attbyval;
  char         attalign;
  char         attstorage;
  char         attcompression;
  bool         attnotnull;
  bool         atthasdef;
  bool         atthasmissing;
  char         attidentity;
  char         attgenerated;
  bool         attisdropped;
  bool         attislocal;
  int32        attinhcount;
  Oid          attcollation;
  /* aclitem[] attacl, text[] attoptions, text[] attfdwoptions, anyarray attmissingval */
  ```
- **Indexes**:
  - `pg_attribute_relid_attnam_index` (2658): unique, (attrelid, attname).
  - `pg_attribute_relid_attnum_index` (2659): unique, (attrelid, attnum).
- **Modification API**: `AddNewAttributeTuples`, `RemoveAttributeById`
  (heap.c).
- **Cache identifier**: `ATTNAME`, `ATTNUM`.
- **Dependencies**: attrelid → pg_class (DEPENDENCY_AUTO inherited from
  the relation's drop), atttypid → pg_type, attcollation → pg_collation.
- **Bootstrap status**: rows are generated by genbki.pl from the .h schema
  declarations; no explicit .dat.

### pg_index (2610) — index metadata

- **Identity**: 2610, header `pg_index.h`, no .dat.
- **Storage flags**: local, regular relfilenode.
- **Schema** (key fields):
  ```c
  Oid          indexrelid;
  Oid          indrelid;
  int16        indnatts;
  int16        indnkeyatts;
  bool         indisunique;
  bool         indnullsnotdistinct;
  bool         indisprimary;
  bool         indisexclusion;
  bool         indimmediate;
  bool         indisclustered;
  bool         indisvalid;
  bool         indcheckxmin;
  bool         indisready;
  bool         indislive;
  bool         indisreplident;
  int2vector   indkey;             /* attnum array */
  oidvector    indcollation;
  oidvector    indclass;
  int2vector   indoption;
  pg_node_tree indexprs;            /* expressions */
  pg_node_tree indpred;             /* partial index predicate */
  ```
- **Indexes**:
  - `pg_index_indexrelid_index` (2678): unique, (indexrelid).
  - `pg_index_indrelid_index` (2679): non-unique, (indrelid).
- **Modification API**: `index_create`, `UpdateIndexRelation`,
  `index_drop`, `index_set_state_flags` (index.c).
- **Cache identifier**: `INDEXRELID`.
- **Dependencies**: indexrelid → pg_class (DEPENDENCY_INTERNAL, becomes
  AUTO during recursive drop), indrelid → pg_class (DEPENDENCY_AUTO).
- **Bootstrap status**: no .dat.

### pg_namespace (2615) — schemas

- **Identity**: 2615, header `pg_namespace.h`, `pg_namespace.dat`,
  `pg_namespace.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  NameData     nspname;
  Oid          nspowner;
  /* aclitem[] nspacl */
  ```
- **Indexes**:
  - `pg_namespace_oid_index` (2684): unique, (oid).
  - `pg_namespace_nspname_index` (2685): unique, (nspname).
- **Modification API**: `NamespaceCreate` (pg_namespace.c),
  `RemoveSchemaById` (namespace.c).
- **Cache identifier**: `NAMESPACEOID`, `NAMESPACENAME`.
- **Dependencies**: nspowner → pg_authid.
- **Bootstrap status**: yes; pg_catalog (11), pg_toast (99),
  information_schema, public are seeded.

### pg_database (1262) — databases

- **Identity**: 1262, header `pg_database.h`, `pg_database.dat`.
- **Storage flags**: shared, mapped.
- **Schema**:
  ```c
  Oid          oid;
  NameData     datname;
  Oid          datdba;
  int32        encoding;
  char         datlocprovider;
  bool         datistemplate;
  bool         datallowconn;
  bool         dathasloginevt;
  int32        datconnlimit;
  Oid          dattablespace;
  /* text datcollate, text datctype, text daticulocale, text daticurules,
     text datcollversion, aclitem[] datacl */
  TransactionId datfrozenxid;
  MultiXactId  datminmxid;
  ```
- **Indexes**:
  - `pg_database_oid_index` (2672): unique, (oid).
  - `pg_database_datname_index` (2671): unique, (datname).
- **Modification API**: `createdb`, `dropdb`, `AlterDatabase`
  (`src/backend/commands/dbcommands.c`); `heap_inplace_update_and_unlock`
  for `datfrozenxid`, `datminmxid`.
- **Cache identifier**: `DATABASEOID`.
- **Dependencies**: shared dependency entries via pg_shdepend on owner.
- **Bootstrap status**: yes; template0, template1, postgres seeded.

### pg_tablespace (1213) — tablespaces

- **Identity**: 1213, shared, mapped.
- **Schema**:
  ```c
  Oid          oid;
  NameData     spcname;
  Oid          spcowner;
  /* aclitem[] spcacl, text[] spcoptions */
  ```
- **Indexes**: `pg_tablespace_oid_index` (2697), `pg_tablespace_spcname_index` (2698).
- **Modification API**: `CreateTableSpace`, `DropTableSpace` (tablespace.c).
- **Cache identifier**: `TABLESPACEOID`.
- **Bootstrap status**: yes; pg_default, pg_global seeded.

### pg_authid (1260) — roles (login users + groups)

- **Identity**: 1260, shared, mapped.
- **Schema**:
  ```c
  Oid          oid;
  NameData     rolname;
  bool         rolsuper;
  bool         rolinherit;
  bool         rolcreaterole;
  bool         rolcreatedb;
  bool         rolcanlogin;
  bool         rolreplication;
  bool         rolbypassrls;
  int32        rolconnlimit;
  /* text rolpassword, timestamptz rolvaliduntil */
  ```
- **Indexes**: `pg_authid_oid_index` (2676), `pg_authid_rolname_index` (2677).
- **Modification API**: CREATE/ALTER/DROP ROLE — `user.c`.
- **Cache identifier**: `AUTHOID`, `AUTHNAME`.
- **Bootstrap status**: yes; PUBLIC, BOOTSTRAP_SUPERUSERID seeded.

### pg_auth_members (1261) — role membership

- **Identity**: 1261, shared, mapped.
- **Schema**:
  ```c
  Oid          oid;
  Oid          roleid;
  Oid          member;
  Oid          grantor;
  bool         admin_option;
  bool         inherit_option;
  bool         set_option;
  ```
- **Indexes**: `pg_auth_members_role_member_index` (2694),
  `pg_auth_members_member_role_index` (2695),
  `pg_auth_members_oid_index` (8395),
  `pg_auth_members_grantor_index` (8396).
- **Modification API**: GRANT / REVOKE on role — `aclchk.c`.

### pg_am (2601) — index/table access methods

- **Identity**: 2601, header `pg_am.h`, `pg_am.dat`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  NameData     amname;
  regproc      amhandler;
  char         amtype;          /* 't' (table) or 'i' (index) */
  ```
- **Indexes**: `pg_am_oid_index` (2651), `pg_am_name_index` (2652).
- **Modification API**: CREATE ACCESS METHOD — `amcmds.c`.
- **Cache identifier**: `AMOID`, `AMNAME`.
- **Bootstrap status**: yes; heap, btree, hash, gist, gin, spgist, brin seeded.

### Cross-references

- `[03 Catalog Data Model](03_catalog_data_model_and_bootstrap.md)` for nailed/shared semantics.
- `[07 Relmapper](07_relmapper.md)` for mapped relfilenodes.
- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` for write paths.


### Sample queries

```sql
-- all user tables in this database
SELECT oid, relname, relnamespace, relfrozenxid
  FROM pg_class
 WHERE relkind = 'r' AND relnamespace > 16384;

-- columns of one table
SELECT attname, atttypid::regtype, attnum, attnotnull
  FROM pg_attribute
 WHERE attrelid = 'mytable'::regclass AND attnum > 0;

-- the indexes belonging to a table
SELECT i.indexrelid::regclass, i.indisunique, i.indisprimary
  FROM pg_index i
 WHERE i.indrelid = 'mytable'::regclass;

-- list all schemas
SELECT oid, nspname FROM pg_namespace ORDER BY nspname;

-- this cluster's databases (run from any database)
SELECT oid, datname, datfrozenxid, datminmxid FROM pg_database;
```


## Section Type system

Catalogs that describe the type system: pg_type, pg_cast, pg_range, pg_enum,
pg_collation, pg_conversion.

### pg_type (1247) — data types

- **Identity**: pg_type, OID 1247, header `pg_type.h`, `pg_type.dat`,
  `pg_type.c` helper.
- **Storage flags**: nailed (`BKI_BOOTSTRAP`), mapped.
- **Schema** (FormData_pg_type — abridged):
  ```c
  Oid          oid;
  NameData     typname;
  Oid          typnamespace;
  Oid          typowner;
  int16        typlen;
  bool         typbyval;
  char         typtype;          /* 'b' base, 'c' composite, 'd' domain, 'e' enum, 'p' pseudo, 'r' range, 'm' multirange */
  char         typcategory;
  bool         typispreferred;
  bool         typisdefined;
  char         typdelim;
  Oid          typrelid;          /* if composite, OID of pg_class row */
  Oid          typsubscript;
  Oid          typelem;
  Oid          typarray;
  regproc      typinput;
  regproc      typoutput;
  regproc      typreceive;
  regproc      typsend;
  regproc      typmodin;
  regproc      typmodout;
  regproc      typanalyze;
  char         typalign;
  char         typstorage;
  bool         typnotnull;
  Oid          typbasetype;
  int32        typtypmod;
  int32        typndims;
  Oid          typcollation;
  /* pg_node_tree typdefaultbin, text typdefault, aclitem[] typacl */
  ```
- **Indexes**:
  - `pg_type_oid_index` (2703): unique, (oid).
  - `pg_type_typname_nsp_index` (2704): unique, (typname, typnamespace).
- **Modification API**: `TypeCreate`, `TypeShellMake`, `TypeRename`,
  `RemoveTypeById` (pg_type.c).
- **Cache identifier**: `TYPEOID`, `TYPENAMENSP`.
- **Dependencies**: typrelid → pg_class (DEPENDENCY_INTERNAL),
  typcollation → pg_collation, typowner → pg_authid (shared),
  typnamespace → pg_namespace, plus per-function dependencies for
  typinput/typoutput/etc.
- **Bootstrap status**: yes; every built-in type ships in pg_type.dat
  (~50 types: bool, int4, text, oid, name, ...).

### pg_cast (2605) — type casts

- **Identity**: 2605, `pg_cast.h`, `pg_cast.dat`, `pg_cast.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  Oid          castsource;
  Oid          casttarget;
  Oid          castfunc;        /* InvalidOid means binary-coercible */
  char         castcontext;     /* 'e' (explicit), 'a' (assignment), 'i' (implicit) */
  char         castmethod;      /* 'f' (function), 'b' (binary), 'i' (I/O) */
  ```
- **Indexes**: `pg_cast_oid_index` (2660), `pg_cast_source_target_index` (2661, unique).
- **Modification API**: `CastCreate`, `RemoveCastById`.
- **Cache identifier**: `CASTSOURCETARGET`.
- **Dependencies**: castsource/casttarget → pg_type, castfunc → pg_proc.
- **Bootstrap status**: yes; ~200 built-in casts.

### pg_range (3541) — range type metadata

- **Identity**: 3541, `pg_range.h`, `pg_range.dat`, `pg_range.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          rngtypid;
  Oid          rngsubtype;
  Oid          rngmultitypid;
  Oid          rngcollation;
  Oid          rngsubopc;
  regproc      rngcanonical;
  regproc      rngsubdiff;
  ```
- **Indexes**: `pg_range_rngtypid_index` (3542, unique), `pg_range_rngmultitypid_index` (2228, unique).
- **Modification API**: `RangeCreate`, `RangeDelete`.
- **Cache identifier**: `RANGETYPE`, `RANGEMULTIRANGE`.
- **Bootstrap status**: yes; built-in range types int4range, int8range, numrange, tsrange, tstzrange, daterange.

### pg_enum (3501) — enum labels

- **Identity**: 3501, `pg_enum.h`, no .dat, `pg_enum.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  Oid          enumtypid;
  float4       enumsortorder;
  NameData     enumlabel;
  ```
- **Indexes**:
  - `pg_enum_oid_index` (3502, unique).
  - `pg_enum_typid_label_index` (3503, unique, (enumtypid, enumlabel)).
  - `pg_enum_typid_sortorder_index` (3534, (enumtypid, enumsortorder)).
- **Modification API**: `EnumValuesCreate`, `AddEnumLabel`,
  `RenameEnumLabel`, `RemoveEnumValueById`.
- **Cache identifier**: `ENUMOID`, `ENUMTYPOIDNAME`.
- **Dependencies**: enumtypid → pg_type (DEPENDENCY_INTERNAL — drop the type to drop labels).
- **Bootstrap status**: no.

### pg_collation (3456) — collations

- **Identity**: 3456, `pg_collation.h`, `pg_collation.dat`, `pg_collation.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  NameData     collname;
  Oid          collnamespace;
  Oid          collowner;
  char         collprovider;     /* 'b' built-in, 'c' libc, 'i' ICU */
  bool         collisdeterministic;
  int32        collencoding;
  /* text collcollate, text collctype, text colliculocale, text collicurules, text collversion */
  ```
- **Indexes**:
  - `pg_collation_oid_index` (3085, unique).
  - `pg_collation_name_enc_nsp_index` (3164, unique, (collname, collencoding, collnamespace)).
- **Modification API**: `CollationCreate`, `pg_collation_rename`.
- **Cache identifier**: `COLLOID`, `COLLNAMEENCNSP`.
- **Bootstrap status**: yes; "C", "POSIX", "default", "ucs_basic" seeded.

### pg_conversion (2607) — encoding conversions

- **Identity**: 2607, `pg_conversion.h`, `pg_conversion.dat`,
  `pg_conversion.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid          oid;
  NameData     conname;
  Oid          connamespace;
  Oid          conowner;
  int32        conforencoding;
  int32        contoencoding;
  Oid          conproc;
  bool         condefault;
  ```
- **Indexes**:
  - `pg_conversion_oid_index` (2669, unique).
  - `pg_conversion_name_nsp_index` (2668, unique, (conname, connamespace)).
  - `pg_conversion_default_index` (2670, (connamespace, conforencoding, contoencoding, oid)).
- **Modification API**: `ConversionCreate`, `RemoveConversionById`.
- **Cache identifier**: `CONVOID`, `CONNAMENSP`, `CONDEFAULT`.
- **Bootstrap status**: yes; many built-in conversions (UTF8 ↔ many encodings).

### Cross-references

- `[05 Catalog Caches](05_catalog_caches.md)` for syscache identifiers.
- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` for the per-catalog helper
  functions (TypeCreate, CastCreate, etc.).


### Sample queries

```sql
-- every numeric base type
SELECT oid::regtype, typname, typlen, typcategory
  FROM pg_type WHERE typcategory = 'N' AND typtype = 'b';

-- enum labels for a given type
SELECT enumlabel, enumsortorder
  FROM pg_enum WHERE enumtypid = 'mood'::regtype
 ORDER BY enumsortorder;

-- range subtypes
SELECT t.typname AS range_type, st.typname AS subtype
  FROM pg_range r JOIN pg_type t ON t.oid = r.rngtypid
                  JOIN pg_type st ON st.oid = r.rngsubtype;

-- type casts
SELECT castsource::regtype, casttarget::regtype, castcontext FROM pg_cast;
```


## Section Functions, operators, and access methods

### pg_proc (1255) — functions and procedures

- **Identity**: pg_proc, OID 1255, header `pg_proc.h`, `pg_proc.dat`,
  `pg_proc.c` helper.
- **Storage flags**: nailed (`BKI_BOOTSTRAP`), mapped.
- **Schema** (FormData_pg_proc — abridged):
  ```c
  Oid           oid;
  NameData      proname;
  Oid           pronamespace;
  Oid           proowner;
  Oid           prolang;
  float4        procost;
  float4        prorows;
  Oid           provariadic;
  regproc       prosupport;
  char          prokind;            /* 'f' func, 'p' procedure, 'a' aggregate, 'w' window */
  bool          prosecdef;
  bool          proleakproof;
  bool          proisstrict;
  bool          proretset;
  char          provolatile;        /* 'i' immut, 's' stable, 'v' volatile */
  char          proparallel;        /* 's' safe, 'r' restricted, 'u' unsafe */
  int16         pronargs;
  int16         pronargdefaults;
  Oid           prorettype;
  oidvector     proargtypes;
  /* Oid[] proallargtypes, char[] proargmodes, text[] proargnames,
     pg_node_tree proargdefaults, Oid[] protrftypes, text prosrc, text probin,
     pg_node_tree prosqlbody, text[] proconfig, aclitem[] proacl */
  ```
- **Indexes**:
  - `pg_proc_oid_index` (2690, unique, (oid)).
  - `pg_proc_proname_args_nsp_index` (2691, unique, (proname, proargtypes, pronamespace)).
- **Modification API**: `ProcedureCreate` (pg_proc.c) — central helper used by
  CREATE FUNCTION / PROCEDURE / AGGREGATE.
- **Cache identifier**: `PROCOID`, `PROCNAMEARGSNSP`.
- **Dependencies**: prolang → pg_language, prorettype → pg_type,
  proargtypes[] → pg_type, pronamespace → pg_namespace, proowner → pg_authid.
- **Bootstrap status**: yes; pg_proc.dat ships every built-in function
  (3000+ rows).

### pg_aggregate (2600) — aggregate functions

- **Identity**: 2600, `pg_aggregate.h`, `pg_aggregate.dat`,
  `pg_aggregate.c`.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  regproc       aggfnoid;            /* the pg_proc oid of the aggregate */
  char          aggkind;             /* 'n' normal, 'o' ordered-set, 'h' hypothetical */
  int16         aggnumdirectargs;
  regproc       aggtransfn;
  regproc       aggfinalfn;
  regproc       aggcombinefn;
  regproc       aggserialfn;
  regproc       aggdeserialfn;
  regproc       aggmtransfn;
  regproc       aggminvtransfn;
  regproc       aggmfinalfn;
  bool          aggfinalextra;
  bool          aggmfinalextra;
  char          aggfinalmodify;
  char          aggmfinalmodify;
  Oid           aggsortop;
  Oid           aggtranstype;
  int32         aggtransspace;
  Oid           aggmtranstype;
  int32         aggmtransspace;
  /* text agginitval, text aggminitval */
  ```
- **Indexes**: `pg_aggregate_fnoid_index` (2650, unique, (aggfnoid)).
- **Modification API**: `AggregateCreate` (pg_aggregate.c).
- **Cache identifier**: `AGGFNOID`.
- **Dependencies**: aggfnoid → pg_proc (DEPENDENCY_INTERNAL), trans/inv/final
  funcs → pg_proc.
- **Bootstrap status**: yes; sum, avg, count, min, max, etc. seeded.

### pg_operator (2617) — operators

- **Identity**: 2617, `pg_operator.h`, `pg_operator.dat`, `pg_operator.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      oprname;
  Oid           oprnamespace;
  Oid           oprowner;
  char          oprkind;           /* 'b' binary, 'l' left-unary */
  bool          oprcanmerge;
  bool          oprcanhash;
  Oid           oprleft;
  Oid           oprright;
  Oid           oprresult;
  Oid           oprcom;             /* commutator */
  Oid           oprnegate;
  regproc       oprcode;            /* implementing function */
  regproc       oprrest;
  regproc       oprjoin;
  ```
- **Indexes**: `pg_operator_oid_index` (2688, unique),
  `pg_operator_oprname_l_r_n_index` (2689, unique, (oprname, oprleft, oprright, oprnamespace)).
- **Modification API**: `OperatorCreate`, `OperatorShellMake`,
  `RemoveOperatorById`.
- **Cache identifier**: `OPEROID`, `OPERNAMENSP`.
- **Dependencies**: oprcode → pg_proc, oprleft/oprright/oprresult → pg_type.
- **Bootstrap status**: yes; ~700 built-in operators.

### pg_amop (2602) — operators in operator families

- **Identity**: 2602, `pg_amop.h`, `pg_amop.dat`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           amopfamily;
  Oid           amoplefttype;
  Oid           amoprighttype;
  int16         amopstrategy;
  char          amoppurpose;        /* 's' search, 'o' order */
  Oid           amopopr;             /* the operator */
  Oid           amopmethod;          /* the index AM */
  Oid           amopsortfamily;
  ```
- **Indexes**:
  - `pg_amop_oid_index` (2756, unique).
  - `pg_amop_fam_strat_index` (2754, (amopfamily, amoplefttype, amoprighttype, amopstrategy)).
  - `pg_amop_opr_fam_index` (2755, (amopopr, amoppurpose, amopfamily)).
- **Cache identifier**: `AMOPOPID`, `AMOPSTRATEGY`.

### pg_amproc (2603) — support procedures in operator families

- **Identity**: 2603, `pg_amproc.h`, `pg_amproc.dat`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           amprocfamily;
  Oid           amproclefttype;
  Oid           amprocrighttype;
  int16         amprocnum;
  regproc       amproc;
  ```
- **Indexes**: `pg_amproc_oid_index` (2757, unique),
  `pg_amproc_fam_proc_index` (2655, (amprocfamily, amproclefttype, amprocrighttype, amprocnum)).
- **Cache identifier**: `AMPROCNUM`.

### pg_opclass (2616) — operator classes

- **Identity**: 2616, `pg_opclass.h`, `pg_opclass.dat`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           opcmethod;
  NameData      opcname;
  Oid           opcnamespace;
  Oid           opcowner;
  Oid           opcfamily;
  Oid           opcintype;
  bool          opcdefault;
  Oid           opckeytype;
  ```
- **Indexes**: `pg_opclass_oid_index` (2687, unique),
  `pg_opclass_am_name_nsp_index` (2686, unique, (opcmethod, opcname, opcnamespace)).
- **Cache identifier**: `CLAOID`, `CLAAMNAMENSP`.

### pg_opfamily (2753) — operator families

- **Identity**: 2753, `pg_opfamily.h`, `pg_opfamily.dat`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           opfmethod;
  NameData      opfname;
  Oid           opfnamespace;
  Oid           opfowner;
  ```
- **Indexes**: `pg_opfamily_oid_index` (2755, unique),
  `pg_opfamily_am_name_nsp_index` (2754, unique, (opfmethod, opfname, opfnamespace)).
- **Cache identifier**: `OPFAMILYOID`, `OPFAMILYAMNAMENSP`.

### pg_language (2612) — procedural languages

- **Identity**: 2612, `pg_language.h`, `pg_language.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      lanname;
  Oid           lanowner;
  bool          lanispl;
  bool          lanpltrusted;
  Oid           lanplcallfoid;
  Oid           laninline;
  Oid           lanvalidator;
  /* aclitem[] lanacl */
  ```
- **Indexes**: `pg_language_oid_index` (2681, unique), `pg_language_name_index` (2682, unique).
- **Cache identifier**: `LANGOID`, `LANGNAME`.
- **Bootstrap status**: yes; internal, c, sql, plpgsql, edb*, etc.

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` for ProcedureCreate,
  AggregateCreate, OperatorCreate.
- `catalog_inventory/type_system.md` for the type-side of operator
  definitions.


### Sample queries

```sql
-- functions named "lower"
SELECT proname, proargtypes, prorettype FROM pg_proc WHERE proname = 'lower';

-- operators on int4
SELECT oprname, oprleft::regtype, oprright::regtype, oprresult::regtype
  FROM pg_operator
 WHERE oprleft = 'int4'::regtype AND oprright = 'int4'::regtype;

-- aggregate functions
SELECT proname, aggtransfn, aggfinalfn
  FROM pg_aggregate JOIN pg_proc ON pg_proc.oid = aggfnoid;

-- index access methods
SELECT amname, amtype FROM pg_am;
```


## Section Constraints, dependencies, defaults, inheritance

### pg_constraint (2606) — CHECK / UNIQUE / FK / EXCLUSION constraints

- **Identity**: 2606, header `pg_constraint.h`, no .dat, `pg_constraint.c`
  helper.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  Oid           oid;
  NameData      conname;
  Oid           connamespace;
  char          contype;          /* 'c'=check, 'p'=primary, 'u'=unique, 'f'=foreign, 't'=trigger, 'x'=exclusion */
  bool          condeferrable;
  bool          condeferred;
  bool          convalidated;
  Oid           conrelid;          /* table */
  Oid           contypid;          /* domain (if domain constraint) */
  Oid           conindid;          /* underlying index */
  Oid           conparentid;
  Oid           confrelid;         /* foreign table (FK) */
  char          confupdtype;
  char          confdeltype;
  char          confmatchtype;
  bool          conislocal;
  int32         coninhcount;
  bool          connoinherit;
  /* int2[] conkey, int2[] confkey, Oid[] conpfeqop, Oid[] conppeqop, Oid[] conffeqop,
     Oid[] confdelsetcols, Oid[] conexclop, pg_node_tree conbin */
  ```
- **Indexes**:
  - `pg_constraint_oid_index` (2667, unique).
  - `pg_constraint_conrelid_contypid_conname_index` (2664, unique).
  - `pg_constraint_conname_nsp_index` (2665, (conname, connamespace)).
  - `pg_constraint_contypid_index` (2666, (contypid)).
- **Modification API**: `CreateConstraintEntry` (pg_constraint.c),
  `RemoveConstraintById`, `RenameConstraintById`.
- **Cache identifier**: `CONSTROID`, `CONSTRRELOID`.
- **Dependencies**: conrelid → pg_class, contypid → pg_type, conindid →
  pg_class, confrelid → pg_class, conkey/confkey/conpfeqop... per-column
  references.

### pg_depend (2608) — object-graph dependencies

- **Identity**: 2608, `pg_depend.h`, no .dat, `pg_depend.c`.
- **Storage flags**: local (one pg_depend per database).
- **Schema**:
  ```c
  Oid           classid;        /* depender's catalog OID */
  Oid           objid;          /* depender's row OID */
  int32         objsubid;       /* column # for cols, 0 otherwise */
  Oid           refclassid;     /* referenced object's catalog OID */
  Oid           refobjid;
  int32         refobjsubid;
  char          deptype;        /* 'n','a','i','p','e','x','P','S' */
  ```
- **Indexes**:
  - `pg_depend_depender_index` (2673, (classid, objid, objsubid)).
  - `pg_depend_reference_index` (2674, (refclassid, refobjid, refobjsubid)).
- **Modification API**: `recordDependencyOn`,
  `recordMultipleDependencies`, `deleteDependencyRecordsFor`,
  `deleteDependencyRecordsForClass`.
- **Cache identifier**: none — dependencies are scanned via index, not cached.
- **Dependencies**: a meta-catalog. Rows in pg_depend describe relationships
  between rows in any other catalog.

### pg_shdepend (1214) — shared (cross-database) dependencies

- **Identity**: 1214, shared, mapped, `pg_shdepend.c`.
- **Schema**:
  ```c
  Oid           dbid;            /* database OID; 0 if shared object */
  Oid           classid;
  Oid           objid;
  int32         objsubid;
  Oid           refclassid;
  Oid           refobjid;
  char          deptype;          /* 'a' SHARED_DEPENDENCY_ACL, 'o' OWNER, etc. */
  ```
- **Indexes**:
  - `pg_shdepend_depender_index` (1232).
  - `pg_shdepend_reference_index` (1233).
- **Modification API**: `recordSharedDependencyOn`,
  `changeDependencyOnOwner`, `changeDependencyOnRole`.
- **Bootstrap status**: shared catalog, mapped.

### pg_attrdef (2604) — column default expressions

- **Identity**: 2604, `pg_attrdef.h`, no .dat, `pg_attrdef.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           adrelid;
  int16         adnum;
  pg_node_tree  adbin;            /* parsed expression */
  ```
- **Indexes**:
  - `pg_attrdef_oid_index` (2657, unique).
  - `pg_attrdef_adrelid_adnum_index` (2656, unique, (adrelid, adnum)).
- **Modification API**: `StoreAttrDefault` (pg_attrdef.c),
  `RemoveAttrDefault`, `RemoveAttrDefaultById`.
- **Cache identifier**: `ATTRDEFOID`.
- **Dependencies**: (adrelid, adnum) → pg_attribute (DEPENDENCY_AUTO).

### pg_inherits (2611) — inheritance / partition relationships

- **Identity**: 2611, `pg_inherits.h`, no .dat, `pg_inherits.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           inhrelid;        /* child */
  Oid           inhparent;       /* parent */
  int32         inhseqno;        /* order among parents (multiple inheritance) */
  bool          inhdetachpending;
  ```
- **Indexes**:
  - `pg_inherits_relid_seqno_index` (2680, unique, (inhrelid, inhseqno)).
  - `pg_inherits_parent_index` (2187, (inhparent)).
- **Modification API**: `StoreSingleInheritance` (pg_inherits.c),
  `RelationRemoveInheritance`, `find_inheritance_children`,
  `find_all_inheritors`.
- **Cache identifier**: none — but the partition-tree code (partcache.c)
  caches results per parent.

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — recordDependencyOn,
  performDeletion, dependency walk algorithm.
- `[Section: Partitioning](#section-partitioning)` — pg_inherits role in partitioning.


### Sample queries

```sql
-- foreign keys
SELECT conname, conrelid::regclass, confrelid::regclass, conkey, confkey
  FROM pg_constraint WHERE contype = 'f';

-- everything that depends on a particular table
SELECT classid::regclass, objid, deptype
  FROM pg_depend
 WHERE refclassid = 'pg_class'::regclass AND refobjid = 'mytable'::regclass;

-- inheritance (or partition) children
SELECT inhrelid::regclass, inhparent::regclass, inhseqno
  FROM pg_inherits;

-- column defaults
SELECT adrelid::regclass, adnum, pg_get_expr(adbin, adrelid) AS default_expr
  FROM pg_attrdef;
```


## Section Partitioning

### pg_partitioned_table (3350) — per-partitioned-table metadata

- **Identity**: 3350, header `pg_partitioned_table.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           partrelid;          /* OID of the partitioned table */
  char          partstrat;          /* 'r' RANGE, 'l' LIST, 'h' HASH */
  int16         partnatts;
  Oid           partdefid;          /* OID of default partition */
  int2vector    partattrs;          /* column attnums; 0 = expression */
  /* Oid[] partclass, Oid[] partcollation, pg_node_tree partexprs */
  ```
- **Indexes**: `pg_partitioned_table_partrelid_index` (3351, unique, (partrelid)).
- **Modification API**:
  - `StorePartitionKey` (`src/backend/catalog/partition.c`).
  - `RemovePartitionKeyByRelId`.
  - `pg_partitioned_table_aclmask` (rare; usually inherits from pg_class).
- **Cache identifier**: `PARTRELID`.
- **Dependencies**: partrelid → pg_class (DEPENDENCY_INTERNAL — drop the
  table to drop this row), partdefid → pg_class.

### pg_inherits (2611)

Already documented in `constraints_and_dependencies.md`. For partitioning,
`pg_inherits` is the source-of-truth for the parent-child relationship:
each partition is a `pg_inherits` row with `inhparent` = partitioned table.

The partition is indicated by `pg_class.relispartition = true`. The
`relpartbound` text node in pg_class describes the bound spec
(`FOR VALUES FROM (...) TO (...)`, etc.).

### pg_class partition flags

`pg_class` carries two partition-related fields:

| Field            | Meaning                                                   |
|------------------|-----------------------------------------------------------|
| `relispartition` | true iff this row is a partition of some parent           |
| `relkind = 'p'`  | this is a partitioned table (has children)                |
| `relpartbound`   | pg_node_tree representing the partition bound expression  |
| `relrewrite`     | for ATTACH PARTITION CONCURRENTLY: temp ID during rewrite |

### In-memory representation

`partcache.c::RelationGetPartitionDesc(rel)` builds a `PartitionDesc` with:

```c
typedef struct PartitionDescData
{
    int                nparts;         /* # partitions */
    bool               detached_exist;  /* any DETACH PENDING child? */
    Oid               *oids;            /* OIDs sorted by bound */
    bool              *is_leaf;
    PartitionBoundInfo boundinfo;        /* the bound-array structure */
} PartitionDescData;
```

Built from a snapshot of pg_inherits + pg_class.relpartbound rows. Cached
on the parent's RelationData via `rd_partdesc`. Invalidated on RELOID
syscache callback.

`partition.c::RelationGetPartitionKey(rel)` builds a `PartitionKey` with
column attnums, collations, opclass, partkey expressions. Cached on
`rd_partkey`.

### Partition-routing

`execPartition.c` uses the cached PartitionDesc + PartitionKey to route
INSERT row tuples to the correct child. Repeated invocations skip the
rebuild via the relcache cache.

### Partition pruning

`partprune.c` reads the same cached structures and produces a list of
"surviving" partition OIDs after evaluating the WHERE clause's
constraints against the partition bounds.

### Cross-references

- `[05 Catalog Caches](05_catalog_caches.md)` — partcache.c.
- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — partition-related DDL paths.
- `[Section: Core Relations](#section-core-relations)` — pg_class, pg_inherits.


### Sample queries

```sql
-- list partitioned tables
SELECT partrelid::regclass, partstrat, partattrs::int2[]
  FROM pg_partitioned_table;

-- list partitions of a partitioned table
SELECT inhrelid::regclass, pg_get_expr(c.relpartbound, c.oid)
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = 'measurement'::regclass;
```

Or use the more friendly view `pg_partition_tree('measurement')`.


## Section Statistics

### pg_statistic (2619) — per-column statistics

- **Identity**: 2619, header `pg_statistic.h`, no .dat.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  Oid           starelid;
  int16         staattnum;
  bool          stainherit;
  float4        stanullfrac;
  int32         stawidth;
  float4        stadistinct;
  int16         stakind1;
  int16         stakind2;
  int16         stakind3;
  int16         stakind4;
  int16         stakind5;
  Oid           staop1;       /* operator OIDs for the slots */
  /* ... staop2..staop5 ... */
  Oid           stacoll1;
  /* ... stacoll2..stacoll5 ... */
  /* float4[]  stanumbers1..5  */
  /* anyarray stavalues1..5    */
  ```

  Each `stakind` slot represents one *kind* of statistic (e.g.,
  STATISTIC_KIND_MCV, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_CORRELATION,
  STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM, ...). The 5-slot design lets
  pg_statistic store multiple kinds without separate rows.

- **Indexes**: `pg_statistic_relid_att_inh_index` (2696, unique,
  (starelid, staattnum, stainherit)).
- **Modification API**:
  - `update_attstats` (analyze.c) — bulk rewrite of pg_statistic rows for
    one relation.
  - `RemoveStatistics` (heap.c) — delete all pg_statistic for a relation
    (called by heap_drop_with_catalog).
- **Cache identifier**: `STATRELATTINH`.
- **Dependencies**: starelid → pg_class (DEPENDENCY_AUTO).
- **Bootstrap status**: no.

### pg_statistic_ext (3381) — extended statistics objects

- **Identity**: 3381, `pg_statistic_ext.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           stxrelid;
  NameData      stxname;
  Oid           stxnamespace;
  Oid           stxowner;
  /* int2[] stxkeys, char[] stxkind, text[] stxstattarget, pg_node_tree stxexprs */
  ```
  `stxkind` is an array of one-letter codes:
  - `'d'` = ndistinct (multivariate ndistinct)
  - `'f'` = functional dependencies
  - `'m'` = MCVs (multi-column most-common values)
  - `'e'` = expression statistics

- **Indexes**:
  - `pg_statistic_ext_oid_index` (3380, unique).
  - `pg_statistic_ext_name_index` (3997, unique, (stxname, stxnamespace)).
  - `pg_statistic_ext_relid_index` (3379, (stxrelid)).
- **Modification API**:
  - `CreateStatistics` (statscmds.c).
  - `RemoveStatisticsExtById`.
  - `AlterStatistics`.
- **Cache identifier**: `STATEXTOID`, `STATEXTNAMENSP`.
- **Dependencies**: stxrelid → pg_class, stxowner → pg_authid.

### pg_statistic_ext_data (3429) — computed extended stats data

- **Identity**: 3429, `pg_statistic_ext_data.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           stxoid;
  bool          stxdinherit;
  /* pg_ndistinct stxdndistinct, pg_dependencies stxddependencies,
     pg_mcv_list stxdmcv, pg_statistic[] stxdexpr */
  ```
- **Indexes**: `pg_statistic_ext_data_stxoid_inh_index` (3430, unique,
  (stxoid, stxdinherit)).
- **Modification API**:
  - `statext_store` (extended_stats.c) — written by ANALYZE.
  - `RemoveStatisticsExtById` removes both pg_statistic_ext and
    pg_statistic_ext_data rows.
- **Cache identifier**: `STATEXTDATASTXOID`.
- **Dependencies**: stxoid → pg_statistic_ext (DEPENDENCY_INTERNAL).

### ANALYZE write path

```
ANALYZE
 -> commands/analyze.c::do_analyze_rel
     -> sample tuples via TableAmRoutine::scan_analyze_next_tuple
     -> compute stats per column (compute_attribute_stats)
     -> CatalogTupleUpdate / Insert into pg_statistic
     -> compute_extension_stats (extended_stats.c)
         -> CatalogTupleUpdate into pg_statistic_ext_data
     -> heap_inplace_update_and_unlock (pg_class.relpages, reltuples,
                                        relallvisible)
```

### Use during planning

Planner's selectivity estimator (`selfuncs.c`) reads pg_statistic via
`get_attstatsslot()`, which uses `SearchSysCache3(STATRELATTINH, ...)`.
Extended stats are read via `statext_clauselist_selectivity()`.

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — heap_inplace_update for
  pg_class statistics.
- `[Section: Core Relations](#section-core-relations)` — pg_class.


### Sample queries

```sql
-- column distinct estimates
SELECT starelid::regclass, staattnum, stanullfrac, stadistinct
  FROM pg_statistic LIMIT 5;

-- extended statistics objects
SELECT stxname, stxrelid::regclass, stxkind, stxkeys
  FROM pg_statistic_ext;

-- ANALYZE writes both pg_statistic and pg_statistic_ext_data
ANALYZE mytable;
```


## Section Access control

Catalogs that govern role membership, ACLs, default privileges, security
labels, RLS policies, and parameter ACLs.

### pg_authid (1260)

See `core_relations.md`. Stores roles (login users + groups). Shared, mapped,
.dat.

### pg_auth_members (1261)

See `core_relations.md`. Role-to-role grant table. Shared, mapped.

### pg_database (1262)

See `core_relations.md`. Per-database privileges live in `datacl`. Shared,
mapped.

### pg_tablespace (1213)

See `core_relations.md`. Tablespace privileges live in `spcacl`.

### pg_default_acl (826) — ALTER DEFAULT PRIVILEGES

- **Identity**: 826, `pg_default_acl.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           defaclrole;
  Oid           defaclnamespace;
  char          defaclobjtype;     /* 'r' rel, 'S' seq, 'f' func, 'T' type, 'n' schema */
  /* aclitem[] defaclacl */
  ```
- **Indexes**:
  - `pg_default_acl_oid_index` (827, unique).
  - `pg_default_acl_role_nsp_obj_index` (828, unique,
    (defaclrole, defaclnamespace, defaclobjtype)).
- **Modification API**: `ExecAlterDefaultPrivilegesStmt`,
  `RemoveDefaultACLById`.
- **Cache identifier**: `DEFACLROLENSPOBJ`.

### pg_init_privs (3394) — original privileges (for pg_dump)

- **Identity**: 3394, `pg_init_privs.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           classoid;
  Oid           objoid;
  int32         objsubid;
  char          privtype;
  /* aclitem[] initprivs */
  ```
  `privtype`: `'i'` (initdb-time) or `'e'` (extension).

- **Indexes**: `pg_init_privs_o_c_o_index` (3395, unique,
  (objoid, classoid, objsubid)).
- **Modification API**: `recordExtensionInitPriv` (extension.c),
  `recordExtObjInitPriv`.

### pg_policy (3256) — RLS policies

- **Identity**: 3256, `pg_policy.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      polname;
  Oid           polrelid;
  char          polcmd;          /* 'r' SELECT, 'a' INSERT, 'w' UPDATE, 'd' DELETE, '*' ALL */
  bool          polpermissive;
  /* Oid[] polroles, pg_node_tree polqual, pg_node_tree polwithcheck */
  ```
- **Indexes**:
  - `pg_policy_oid_index` (3257, unique).
  - `pg_policy_polrelid_polname_index` (3258, unique,
    (polrelid, polname)).
- **Modification API**: `CreatePolicy`, `RenamePolicy`, `RemovePolicyById`
  (`commands/policy.c`).
- **Cache identifier**: `POLICYOID`.
- **Dependencies**: polrelid → pg_class (DEPENDENCY_AUTO),
  polroles[] → pg_authid via pg_shdepend.

### pg_seclabel (3596) — security labels

- **Identity**: 3596, `pg_seclabel.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  int32         objsubid;
  text          provider;        /* "selinux", etc. */
  text          label;
  ```
- **Indexes**: `pg_seclabel_object_index` (3597, unique,
  (objoid, classoid, objsubid, provider)).
- **Modification API**: `SetSecurityLabel`, `DeleteSecurityLabel`.

### pg_shseclabel (3592) — security labels for shared objects

- **Identity**: 3592, shared, mapped.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  text          provider;
  text          label;
  ```
- **Indexes**: `pg_shseclabel_object_index` (3593, unique).
- **Modification API**: same as pg_seclabel; routed by class.

### pg_parameter_acl (6243) — ACLs on configuration parameters

- **Identity**: 6243, shared, mapped, `pg_parameter_acl.c`.
- **Schema**:
  ```c
  Oid           oid;
  text          parname;
  /* aclitem[] paracl */
  ```
- **Indexes**:
  - `pg_parameter_acl_oid_index` (6244, unique).
  - `pg_parameter_acl_parname_index` (6245, unique).
- **Modification API**: `ParameterAclCreate`, `ParameterAclLookup`.
- **Cache identifier**: `PARAMETERACLOID`, `PARAMETERACLNAME`.

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — `aclchk.c::ExecGrantStmt_oids`.
- `[05 Catalog Caches](05_catalog_caches.md)` — RLS policy invalidation via partcache.


### Sample queries

```sql
-- list all roles
SELECT rolname, rolsuper, rolcreaterole FROM pg_authid;

-- per-database default privileges
SELECT defaclrole::regrole, defaclnamespace::regnamespace, defaclobjtype
  FROM pg_default_acl;

-- RLS policies
SELECT polname, polrelid::regclass, polcmd, polpermissive
  FROM pg_policy;

-- security labels (sepgsql etc.)
SELECT classoid::regclass, objoid, provider, label FROM pg_seclabel;
```


## Section Replication and publication

### pg_publication (6104) — logical-replication publications

- **Identity**: 6104, `pg_publication.h`, no .dat, `pg_publication.c`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      pubname;
  Oid           pubowner;
  bool          puballtables;
  bool          puballsequences;
  bool          pubinsert;
  bool          pubupdate;
  bool          pubdelete;
  bool          pubtruncate;
  bool          pubviaroot;
  ```
- **Indexes**:
  - `pg_publication_oid_index` (6110, unique).
  - `pg_publication_pubname_index` (6111, unique).
- **Modification API**: `CreatePublication`, `AlterPublication`,
  `RemovePublicationById`.
- **Cache identifier**: `PUBLICATIONOID`, `PUBLICATIONNAME`.

### pg_publication_rel (6106) — publications → tables mapping

- **Identity**: 6106, `pg_publication_rel.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           prpubid;          /* publication */
  Oid           prrelid;          /* relation */
  /* pg_node_tree prqual, int2vector prattrs */
  ```
- **Indexes**:
  - `pg_publication_rel_oid_index` (6112, unique).
  - `pg_publication_rel_prrelid_prpubid_index` (6113, unique).
- **Modification API**: `publication_add_relation`,
  `RemovePublicationRelById`.
- **Cache identifier**: `PUBLICATIONRELMAP`, `PUBLICATIONREL`.

### pg_publication_namespace (6237) — publications → schemas

- **Identity**: 6237, `pg_publication_namespace.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  Oid           pnpubid;
  Oid           pnnspid;
  ```
- **Indexes**:
  - `pg_publication_namespace_object_index` (6238, unique).
  - `pg_publication_namespace_pnnspid_pnpubid_index` (6239, unique).
- **Modification API**: `publication_add_schema`,
  `RemovePublicationSchemaById`.
- **Cache identifier**: `PUBLICATIONNAMESPACE`, `PUBLICATIONNAMESPACEMAP`.

### pg_subscription (6100) — logical-replication subscriptions

- **Identity**: 6100, shared, mapped, `pg_subscription.c`.
- **Schema**:
  ```c
  Oid           oid;
  Oid           subdbid;
  int32         subskiplsn;
  NameData      subname;
  Oid           subowner;
  bool          subenabled;
  bool          subbinary;
  bool          substream;
  bool          subtwophasestate;
  bool          subdisableonerr;
  bool          subpasswordrequired;
  bool          subrunasowner;
  bool          subfailover;
  /* text subconninfo, NameData subslotname, text subsynccommit, text[] subpublications,
     text suborigin */
  ```
- **Indexes**:
  - `pg_subscription_oid_index` (6114, unique).
  - `pg_subscription_subname_index` (6115, unique, (subdbid, subname)).
- **Modification API**: `CreateSubscription`, `AlterSubscription`,
  `DropSubscription` (`commands/subscriptioncmds.c`).
- **Cache identifier**: `SUBSCRIPTIONOID`, `SUBSCRIPTIONNAME`.

### pg_subscription_rel (6102) — per-subscription per-relation state

- **Identity**: 6102, `pg_subscription_rel.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           srsubid;
  Oid           srrelid;
  char          srsubstate;        /* 'i' init, 'd' data sync, 's' synced, 'r' ready */
  XLogRecPtr    srsublsn;
  ```
- **Indexes**: `pg_subscription_rel_srrelid_srsubid_index` (6117, unique,
  (srrelid, srsubid)).
- **Modification API**: `AddSubscriptionRelState`,
  `UpdateSubscriptionRelState`.
- **Cache identifier**: `SUBSCRIPTIONRELMAP`.

### pg_replication_origin (6000) — replication origins

- **Identity**: 6000, shared, mapped.
- **Schema**:
  ```c
  Oid           roident;
  /* text roname */
  ```
- **Indexes**:
  - `pg_replication_origin_roident_index` (6001, unique).
  - `pg_replication_origin_roname_index` (6002, unique).
- **Modification API**: `replorigin_create`, `replorigin_drop_by_name`
  (`replication/logical/origin.c`).
- **Cache identifier**: `REPLORIGIDENT`, `REPLORIGNAME`.

### Cross-references

- `[15 Persistence and WAL Records](15_persistence_and_wal_records.md)` — XLOG_XACT_COMMIT carries
  RepOriginId (used by pg_subscription's `subskiplsn`).
- `[11 Commit Timestamps](11_commit_timestamps.md)` — RepOriginId stored alongside commit timestamps.


### Sample queries

```sql
-- all publications
SELECT pubname, puballtables, pubinsert, pubupdate FROM pg_publication;

-- subscriptions (visible only to superusers)
SELECT subname, subdbid, subenabled, subconninfo FROM pg_subscription;

-- replication origins
SELECT roident, roname FROM pg_replication_origin;
```


## Section Triggers and rewrite rules

### pg_trigger (2620) — triggers

- **Identity**: 2620, `pg_trigger.h`, no .dat.
- **Storage flags**: local.
- **Schema** (key fields):
  ```c
  Oid           oid;
  Oid           tgrelid;
  Oid           tgparentid;
  NameData      tgname;
  Oid           tgfoid;             /* trigger function */
  int16         tgtype;              /* bitmask: BEFORE/AFTER/INSTEAD; INSERT/UPDATE/DELETE/TRUNCATE; ROW/STATEMENT */
  char          tgenabled;           /* 'O' origin, 'A' always, 'R' replica, 'D' disabled */
  bool          tgisinternal;
  bool          tgisclone;
  Oid           tgconstrrelid;
  Oid           tgconstrindid;
  Oid           tgconstraint;
  bool          tgdeferrable;
  bool          tginitdeferred;
  int16         tgnargs;
  /* int2vector tgattr, bytea tgargs, pg_node_tree tgqual, NameData tgoldtable, NameData tgnewtable */
  ```
- **Indexes**:
  - `pg_trigger_oid_index` (2702, unique).
  - `pg_trigger_tgrelid_tgname_index` (2701, unique, (tgrelid, tgname)).
  - `pg_trigger_tgconstraint_index` (2699, (tgconstraint)).
- **Modification API**: `CreateTrigger` (`trigger.c`),
  `RemoveTriggerById`, `EnableDisableTrigger`.
- **Cache identifier**: `TRGOID`, `TRGRELID`.
- **Dependencies**: tgrelid → pg_class (DEPENDENCY_AUTO),
  tgfoid → pg_proc, tgconstraint → pg_constraint.

### pg_event_trigger (3466) — event-trigger registrations

- **Identity**: 3466, `pg_event_trigger.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      evtname;
  NameData      evtevent;
  Oid           evtowner;
  Oid           evtfoid;
  char          evtenabled;
  /* text[] evttags */
  ```
- **Indexes**:
  - `pg_event_trigger_evtname_index` (3467, unique).
  - `pg_event_trigger_oid_index` (3468, unique).
- **Modification API**: `CreateEventTrigger`,
  `RemoveEventTriggerById`.
- **Cache identifier**: `EVENTTRIGGEROID`, `EVENTTRIGGERNAME`.

### pg_rewrite (2618) — rewrite rules (views, ON SELECT/INSERT rules)

- **Identity**: 2618, `pg_rewrite.h`, no .dat.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      rulename;
  Oid           ev_class;          /* relation */
  char          ev_type;           /* '1' SELECT, '2' UPDATE, '3' INSERT, '4' DELETE */
  char          ev_enabled;
  bool          is_instead;
  /* pg_node_tree ev_qual, pg_node_tree ev_action */
  ```
- **Indexes**:
  - `pg_rewrite_oid_index` (2692, unique).
  - `pg_rewrite_rel_rulename_index` (2693, unique, (ev_class, rulename)).
- **Modification API**: `DefineRule`, `RewriteQuery`,
  `RemoveRewriteRuleById`.
- **Cache identifier**: `RULERELNAME`.
- **Dependencies**: ev_class → pg_class (DEPENDENCY_AUTO).

### In-memory representations

- Triggers: `RelationData::trigdesc` (TriggerDesc) — built from pg_trigger.
- Event triggers: cached via `evtcache.c::EventCacheLookup`.
- Rewrite rules: `RelationData::rd_rules` (RuleLock).

### Cross-references

- `[05 Catalog Caches](05_catalog_caches.md)` — evtcache, RelationData::trigdesc.
- `[Section: Core Relations](#section-core-relations)` — pg_class.relhastriggers,
  relhasrules.


### Sample queries

```sql
-- triggers on a particular table
SELECT tgname, tgtype, tgenabled, tgfoid::regproc
  FROM pg_trigger WHERE tgrelid = 'mytable'::regclass;

-- event triggers (fire on DDL events)
SELECT evtname, evtevent, evtfoid::regproc, evtenabled FROM pg_event_trigger;

-- view rewrite rules
SELECT rulename, ev_class::regclass, ev_type, is_instead FROM pg_rewrite;
```


## Section Extensions and foreign data wrappers

### pg_extension (3079) — installed extensions

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

### pg_foreign_data_wrapper (2328) — FDW handlers

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

### pg_foreign_server (1417) — foreign servers

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

### pg_foreign_table (3118) — foreign-table options

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

### pg_user_mapping (1418) — user mappings for foreign servers

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

### pg_transform (3576) — datatype transforms for procedural languages

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

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — extension installation,
  CREATE FOREIGN TABLE flow.


### Sample queries

```sql
-- installed extensions
SELECT extname, extversion FROM pg_extension;

-- foreign servers and the FDW they use
SELECT s.srvname, w.fdwname FROM pg_foreign_server s
   JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw;

-- user mappings
SELECT umuser::regrole, umserver FROM pg_user_mapping;
```

DDL example: `CREATE EXTENSION pgcrypto;` populates pg_extension and
attaches `DEPENDENCY_EXTENSION` rows in pg_depend for every object the
extension created.


## Section Text search

### pg_ts_config (3602) — text-search configurations

- **Identity**: 3602, `pg_ts_config.h`, `pg_ts_config.dat`.
- **Storage flags**: local.
- **Schema**:
  ```c
  Oid           oid;
  NameData      cfgname;
  Oid           cfgnamespace;
  Oid           cfgowner;
  Oid           cfgparser;
  ```
- **Indexes**:
  - `pg_ts_config_oid_index` (3712, unique).
  - `pg_ts_config_cfgname_index` (3608, unique, (cfgname, cfgnamespace)).
- **Cache identifier**: `TSCONFIGOID`, `TSCONFIGNAMENSP`.
- **Bootstrap status**: yes; ~30 built-in configurations (english,
  french, german, simple, ...).

### pg_ts_config_map (3603) — TS-config token-type → dictionary maps

- **Identity**: 3603, `pg_ts_config_map.h`, `pg_ts_config_map.dat`.
- **Schema**:
  ```c
  Oid           mapcfg;
  int32         maptokentype;
  int32         mapseqno;
  Oid           mapdict;
  ```
- **Indexes**: `pg_ts_config_map_index` (3609, unique,
  (mapcfg, maptokentype, mapseqno)).
- **Cache identifier**: `TSCONFIGMAP`.

### pg_ts_dict (3600) — text-search dictionaries

- **Identity**: 3600, `pg_ts_dict.h`, `pg_ts_dict.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      dictname;
  Oid           dictnamespace;
  Oid           dictowner;
  Oid           dicttemplate;
  /* text dictinitoption */
  ```
- **Indexes**:
  - `pg_ts_dict_oid_index` (3604, unique).
  - `pg_ts_dict_dictname_index` (3605, unique, (dictname, dictnamespace)).
- **Cache identifier**: `TSDICTOID`, `TSDICTNAMENSP`.

### pg_ts_parser (3601) — text-search parsers

- **Identity**: 3601, `pg_ts_parser.h`, `pg_ts_parser.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      prsname;
  Oid           prsnamespace;
  regproc       prsstart;
  regproc       prstoken;
  regproc       prsend;
  regproc       prsheadline;
  regproc       prslextype;
  ```
- **Indexes**:
  - `pg_ts_parser_oid_index` (3606, unique).
  - `pg_ts_parser_prsname_index` (3607, unique, (prsname, prsnamespace)).
- **Cache identifier**: `TSPARSEROID`, `TSPARSERNAMENSP`.

### pg_ts_template (3764) — text-search templates

- **Identity**: 3764, `pg_ts_template.h`, `pg_ts_template.dat`.
- **Schema**:
  ```c
  Oid           oid;
  NameData      tmplname;
  Oid           tmplnamespace;
  regproc       tmplinit;
  regproc       tmpllexize;
  ```
- **Indexes**:
  - `pg_ts_template_oid_index` (3766, unique).
  - `pg_ts_template_tmplname_index` (3765, unique).
- **Cache identifier**: `TSTEMPLATEOID`, `TSTEMPLATENAMENSP`.

### In-memory caching

`ts_cache.c` builds `TSConfigCacheEntry`, `TSDictionaryCacheEntry`,
`TSParserCacheEntry` per pg_ts_* OID. Invalidated via TSCONFIGOID, TSDICTOID,
TSPARSEROID syscache callbacks.

### Cross-references

- `[05 Catalog Caches](05_catalog_caches.md)` — ts_cache.c.


### Sample queries

```sql
-- list TS configurations
SELECT cfgname, cfgnamespace::regnamespace FROM pg_ts_config;

-- map of token-types to dictionaries for the english config
SELECT maptokentype, mapdict::regdictionary
  FROM pg_ts_config_map
 WHERE mapcfg = 'english'::regconfig
 ORDER BY maptokentype, mapseqno;

-- TS dictionaries
SELECT dictname, dicttemplate::regdictionary FROM pg_ts_dict;
```


## Section Miscellaneous

Catalogs that don't fit cleanly into other groupings: large objects,
descriptions, GUC defaults, sequences.

### pg_largeobject (2613) — large object data chunks

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

### pg_largeobject_metadata (2995) — large object owner + ACL

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

### pg_db_role_setting (2964) — ALTER DATABASE/ROLE SET defaults

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

### pg_description (2609) — COMMENTs on non-shared objects

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

### pg_shdescription (2396) — COMMENTs on shared objects

- **Identity**: 2396, shared, mapped.
- **Schema**:
  ```c
  Oid           objoid;
  Oid           classoid;
  text          description;
  ```
- **Indexes**: `pg_shdescription_o_c_index` (2397, unique).
- **Modification API**: `CreateSharedComments`, `DeleteSharedComments`.

### pg_sequence (2224) — sequence metadata

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

### Cross-references

- `[04 Catalog Modification APIs](04_catalog_modification_apis.md)` — sequence DDL.
- `[Section: Core Relations](#section-core-relations)` — pg_class with relkind = 'S'.


### Sample queries

```sql
-- COMMENTs on objects
SELECT classoid::regclass, objoid, description
  FROM pg_description WHERE classoid = 'pg_class'::regclass;

-- sequences in this database (relkind 'S' in pg_class)
SELECT seqrelid::regclass, seqstart, seqincrement, seqmin, seqmax
  FROM pg_sequence;

-- per-database/per-role GUC overrides (from ALTER ROLE/DATABASE SET)
SELECT setdatabase::regdatabase, setrole::regrole, setconfig
  FROM pg_db_role_setting;

-- large objects ACL (chunks live in pg_largeobject, indexed by loid+pageno)
SELECT oid AS loid, lomowner::regrole FROM pg_largeobject_metadata;
```


---

[Up: index.md](index.md)  |  [Prev: 17 Hooks and Extensibility](17_hooks_and_extensibility.md)  |  [Next: 19 SLRU Users Catalog](19_slru_users_catalog.md)
