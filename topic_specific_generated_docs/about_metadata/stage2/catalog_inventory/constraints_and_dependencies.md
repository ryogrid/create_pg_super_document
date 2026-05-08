# Catalog Inventory: Constraints, Dependencies, Defaults, Inheritance

## pg_constraint (2606) — CHECK / UNIQUE / FK / EXCLUSION constraints

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

## pg_depend (2608) — object-graph dependencies

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

## pg_shdepend (1214) — shared (cross-database) dependencies

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

## pg_attrdef (2604) — column default expressions

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

## pg_inherits (2611) — inheritance / partition relationships

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

## Cross-references

- `component_catalog_modification_apis.md` — recordDependencyOn,
  performDeletion, dependency walk algorithm.
- `catalog_inventory/partitioning.md` — pg_inherits role in partitioning.
