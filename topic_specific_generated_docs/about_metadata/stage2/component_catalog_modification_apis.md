# Component: Catalog Modification APIs

[Top: ../README.md](../../README.md)

## Overview

Every change to a system catalog must go through one of three "sanctioned"
mutators in `src/backend/catalog/indexing.c`:

```
CatalogTupleInsert        — new row
CatalogTupleUpdate        — replace row
CatalogTupleDelete        — remove row
```

Plain `heap_insert` / `heap_update` / `heap_delete` against a system catalog is
a bug. The sanctioned wrappers do three things:

1. open the catalog's indexes (or accept a pre-opened `CatalogIndexState`),
2. perform the actual heap mutation,
3. fire `CacheInvalidateHeapTuple()`, which queues catcache and relcache
   invalidation messages that travel with the transaction's commit record.

The high-level callers are spread across `heap.c`, `index.c`, `dependency.c`,
`namespace.c`, `storage.c`, `toasting.c`, `aclchk.c`, `objectaddress.c`, plus
the per-catalog helpers in `pg_*.c`.

## Architecture

```mermaid
flowchart TB
    subgraph DDL["DDL drivers"]
        CT[CREATE TABLE -> heap_create_with_catalog]
        CI[CREATE INDEX -> index_create]
        DT[DROP -> performDeletion]
        AT[ALTER -> AlterTableInternal]
        GR[GRANT -> ExecGrantStmt_oids]
        AS[ALTER SCHEMA -> namespace.c]
    end

    subgraph PERCAT["per-catalog helpers"]
        PCREATE["ProcedureCreate, TypeCreate,<br/>CreateConstraintEntry, ..."]
    end

    subgraph CORE["core catalog infrastructure"]
        SAN[CatalogTuple{Insert,Update,Delete}<br/>indexing.c]
        DEP[recordDependency*<br/>dependency.c, pg_depend.c]
        STORE["RelationCreateStorage<br/>RelationDropStorage<br/>storage.c"]
        TOAST[NewHeapCreateToastTable<br/>toasting.c]
    end

    subgraph PHYS["physical layer"]
        HEAP[heap_insert/update/delete]
        IDX[CatalogIndexInsert]
        SMGR[smgrcreate, smgrunlink]
        WAL[XLogInsert]
    end

    DDL --> PCREATE
    DDL --> SAN
    DDL --> DEP
    DDL --> STORE
    DDL --> TOAST
    PCREATE --> SAN
    SAN --> HEAP
    SAN --> IDX
    STORE --> SMGR
    SMGR --> WAL
    HEAP --> WAL
```

## indexing.c — the sanctioned mutators

### CatalogTupleInsert  (importance 0.92, Tier 1)

**Signature** (`indexing.c:233`):
```c
void CatalogTupleInsert(Relation heapRel, HeapTuple tup);
```

**What it does** (verbatim from indexing.c, annotated):
```c
CatalogTupleCheckConstraints(heapRel, tup);   /* CHECK / NOT NULL */
indstate = CatalogOpenIndexes(heapRel);        /* open all indexes once */
simple_heap_insert(heapRel, tup);              /* heap_insert with CID */
CatalogIndexInsert(indstate, tup, TU_All);     /* update every index */
CatalogCloseIndexes(indstate);
```

`simple_heap_insert` is `heap_insert(... HEAP_INSERT_SKIP_FSM, ...)` — for
catalogs we deliberately skip FSM consultation and grow the relation linearly.
`CatalogCloseIndexes` calls `index_close(idx, NoLock)` — the higher-level
caller already holds the catalog's `RowExclusiveLock`.

`simple_heap_insert` itself, before returning, calls `CacheInvalidateHeapTuple`
for catalogs that have a relevant catcache (the relcache callback in heap_modify
hands off to inval.c). This means CCI-visible cache effects are queued the
moment the tuple lands in the buffer.

**Performance note**: `CatalogOpenIndexes` is "moderately expensive" because
it builds the `IndexInfo` list. Callers that insert many rows should switch to
`CatalogTupleInsertWithInfo` (`indexing.c:255`) and amortize the cost.

**Persistence invariant**: This function does not flush WAL itself; the
`heap_insert` it drives writes a normal heap WAL record (RM_HEAP) with the
tuple. Durability is achieved by the eventual `XLOG_XACT_COMMIT` flush at
`RecordTransactionCommit`.

### CatalogTupleUpdate  (importance 0.88, Tier 1)

**Signature** (`indexing.c:313`):
```c
void CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup);
```

Wraps `simple_heap_update` (which advances the tuple's xmin like an ordinary
MVCC update) and reinserts the tuple into every index. The `TU_UpdateIndexes`
out-parameter from `simple_heap_update` is honored: HOT updates are still
allowed and will skip index work for non-key columns.

**Important exception**: VACUUM updates `pg_class.relfrozenxid`,
`pg_class.relminmxid`, `pg_database.datfrozenxid` *in place*, bypassing
CatalogTupleUpdate. The function `heap_inplace_update_and_unlock` (heapam.c)
overwrites the tuple's payload without a new CID and emits a small in-place
update WAL record. See `IsInplaceUpdateRelation` in heap.c.

### CatalogTupleDelete  (importance 0.85, Tier 1)

**Signature** (`indexing.c:365`):
```c
void CatalogTupleDelete(Relation heapRel, ItemPointer tid);
```

Surprisingly small: just `simple_heap_delete(heapRel, tid)`. There is no index
work because dead heap tuples leave behind dead index pointers that VACUUM
cleans up later (`btvacuumcleanup` etc.). The hook that makes this safe is
`CacheInvalidateHeapTuple` inside `simple_heap_delete`, which queues a tuple-by-
tuple catcache flush so concurrent backends do not return the now-deleted row.

### CatalogTuplesMultiInsertWithInfo  (importance 0.5)

`indexing.c:273`. Used by callers that have prepared a TupleTableSlot batch
(notably initdb time and partition-key copying). Calls `heap_multi_insert`
plus per-row `CatalogIndexInsert`.

## heap.c — table-level catalog operations

### heap_create_with_catalog  (importance 0.92, Tier 1)

**Signature** (`heap.c:1105`):
```c
Oid heap_create_with_catalog(
    const char *relname, Oid relnamespace, Oid reltablespace,
    Oid relid, Oid reltypeid, Oid reloftypeid, Oid ownerid,
    Oid accessmtd, TupleDesc tupdesc, List *cooked_constraints,
    char relkind, char relpersistence,
    bool shared_relation, bool mapped_relation,
    OnCommitAction oncommit, Datum reloptions,
    bool use_user_acl, bool allow_system_table_mods,
    bool is_internal, Oid relrewrite,
    ObjectAddress *typaddress);
```

This is the entry point the planner-level CREATE TABLE eventually calls
(via `DefineRelation` -> ... -> `heap_create_with_catalog`).

**Logic walkthrough**:

1. Open `pg_class` with `RowExclusiveLock`.
2. `CheckAttributeNamesTypes(tupdesc, relkind)` — reject duplicate column names,
   pseudotype columns, etc.
3. Reject if a relation with that name already exists in the namespace
   (`get_relname_relid`).
4. Reject if a `pg_type` row of the same name in the same schema already
   exists (and is not an autogen array — auto-rename arrays via
   `moveArrayTypeName`).
5. If `shared_relation` is true, enforce `pg_global` tablespace.
6. Allocate the relation OID via `GetNewOidWithIndex(pg_class, ...)` if not
   pre-supplied; for upgrade scenarios the
   `binary_upgrade_next_heap_pg_class_oid` GUC pins the OID.
7. Compute `relfrozenxid` / `relminmxid` for the new relation
   (`RecentXmin` for ordinary user relations, `FirstNormalTransactionId` for
   shared catalogs).
8. Call `heap_create()` (`heap.c:290`) to allocate the in-memory `Relation`
   object and create the storage (it calls `RelationCreateStorage` which
   emits `XLOG_SMGR_CREATE`).
9. `AddNewRelationType()` — insert the rowtype into `pg_type` (this returns
   the new pg_type OID for `relid -> reltypeid` linkage).
10. `AddNewRelationTuple()` (`heap.c:969`) — insert the `pg_class` row via
    `InsertPgClassTuple` -> `CatalogTupleInsert(pg_class)`.
11. `AddNewAttributeTuples()` (`heap.c:821`) — insert one `pg_attribute` row per
    column via `CatalogTupleInsert(pg_attribute)`.
12. `StoreConstraints()` if there are CHECK or NOT NULL constraints — inserts
    `pg_constraint` rows and `recordDependencyOn(...)` to point at them.
13. Record dependencies: relation -> namespace, relation -> owner,
    relation -> type, relation -> AM. Each `recordDependencyOn` is one
    `CatalogTupleInsert(pg_depend)`.
14. `register_on_commit_action()` if `oncommit != ONCOMMIT_NOOP` (temp tables).
15. Close `pg_class`, return the new OID.

**Performance characteristics**:

- O(ncols) `CatalogTupleInsert` calls into `pg_attribute`. Each one goes
  through `simple_heap_insert + CatalogIndexInsert`. For wide tables, opening
  the indexes once and using `CatalogTuplesMultiInsertWithInfo` would help,
  but as of current code we go one tuple at a time.
- `CacheInvalidateHeapTuple` is called for each insert; messages accumulate in
  `TransInvalidationInfo` until commit.

**Persistence invariants**:

- All catalog inserts are normal heap inserts; durability piggybacks on the
  transaction's commit WAL flush.
- `RelationCreateStorage` queues a pending-delete entry on abort
  (`pendingDeletes` list); the file is unlinked by `smgrDoPendingDeletes` on
  abort. On commit, the file persists.
- The `pg_class` row's `relfilenode` is set equal to the relation OID at create
  time (unless mapped — see `component_relmapper.md`).

### heap_drop_with_catalog  (importance 0.85, Tier 1)

**Signature** (`heap.c:1767`):
```c
void heap_drop_with_catalog(Oid relid);
```

**Logic**:

1. `RelationIdGetRelation(relid)` — open the relation; bumps refcount.
2. `RelationDropStorage(rel)` — schedule the relfilenode unlink at commit
   (does NOT unlink immediately; queues a pending-delete entry).
3. `CountDependentObjects()` — sanity check (caller should have already
   walked the dependency graph).
4. `RemoveStatistics()` — delete `pg_statistic` rows for this relation.
5. `RelationForgetRelation(relid)` — invalidate the relcache entry locally.
6. `RemoveAttributeById()` for each column — `CatalogTupleDelete(pg_attribute)`.
7. `DeleteRelationTuple()` — `CatalogTupleDelete(pg_class)`.
8. `DeleteAttrDefault()` — pg_attrdef rows.
9. `RelationClose(rel)`.

The actual file unlink happens in `smgrDoPendingDeletes` at commit (or inside
`xact_redo_commit` on a standby). Until commit, the file is still on disk;
abort discards the catalog row and leaves the file untouched.

### Helpers worth knowing

| Function                       | Purpose                                                   |
|--------------------------------|-----------------------------------------------------------|
| `heap_create` (heap.c:290)     | physical+memory: allocate Relation, smgrcreate            |
| `InsertPgClassTuple` (896)     | format pg_class row, CatalogTupleInsert                   |
| `AddNewRelationTuple` (969)    | wraps InsertPgClassTuple with all the field defaults      |
| `AddNewAttributeTuples` (821)  | per-column pg_attribute writer                            |
| `StoreConstraints` (~2920)     | inserts pg_constraint rows, records dependencies          |
| `RemoveAttributeById` (1946)   | delete pg_attribute row by (relid, attnum)                |
| `DeleteRelationTuple` (1767+)  | helper called by heap_drop_with_catalog                   |
| `RelationRemoveInheritance`    | `pg_inherits` cleanup at drop time                        |
| `CheckAttributeType`           | type-suitability check during column add                  |

## index.c — index DDL

### index_create  (importance 0.90, Tier 1)

**Signature** (`index.c:~700`):
```c
Oid index_create(Relation heapRelation,
                 const char *indexRelationName,
                 Oid indexRelationId, Oid parentIndexRelid,
                 Oid parentConstraintId, RelFileNumber relFileNumber,
                 IndexInfo *indexInfo, List *indexColNames,
                 Oid accessMethodId, Oid tableSpaceId,
                 Oid *collationIds, Oid *opclassIds,
                 Datum *opclassOptions, int16 *coloptions,
                 NullableDatum *stattargets, Datum reloptions,
                 bits16 flags, bits16 constr_flags,
                 bool allow_system_table_mods, bool is_internal,
                 Oid *constraintId);
```

**Logic**:

1. Allocate the index relation OID (or accept the supplied one).
2. `heap_create()` to allocate the index relation object and storage.
3. Insert the `pg_class` row for the index — `InsertPgClassTuple`.
4. Insert per-attribute rows in `pg_attribute` — `AddNewAttributeTuples`.
5. Insert the `pg_index` row (`UpdateIndexRelation`) — this carries
   `indkey`, `indclass`, `indpred`, `indisvalid`, `indisready`, `indislive`.
6. `recordDependencyOn(index, table, DEPENDENCY_AUTO)` — drop-cascade contract.
7. `recordDependencyOnSingleRelExpr(...)` for predicates / expressions.
8. `index_build()` if not deferred — actually populates the index file.
9. `CacheInvalidateRelcache(heapRelation)` — invalidates the heap's relcache
   so future opens see the new index.

### Other index.c functions

- `index_drop` — drop pg_index, pg_class, pg_attribute rows; `RelationDropStorage`.
- `index_constraint_create` — link the index to a `pg_constraint` UNIQUE/PRIMARY/EXCLUSION row.
- `index_update_stats` — `pg_class.reltuples / relpages` update via `heap_inplace_update_and_unlock`.
- `IndexSetParentIndex` — partition-tree indexes: link a child to its parent.
- `ReindexRelationConcurrently` — orchestrates the multi-step REINDEX CONCURRENTLY protocol.

## dependency.c — the object graph

### recordDependencyOn  (importance 0.85, Tier 1)

**Signature** (`dependency.c`):
```c
void recordDependencyOn(const ObjectAddress *depender,
                        const ObjectAddress *referenced,
                        DependencyType behavior);
```

Inserts one row into `pg_depend`:

| pg_depend column | Source                                        |
|------------------|-----------------------------------------------|
| classid          | depender->classId   (e.g. RelationRelationId) |
| objid            | depender->objectId                            |
| objsubid         | depender->objectSubId (column # for cols)     |
| refclassid       | referenced->classId                           |
| refobjid         | referenced->objectId                          |
| refobjsubid      | referenced->objectSubId                       |
| deptype          | behavior code (n,a,i,p,e,x,...)                |

Dependency types from `src/include/catalog/dependency.h`:

- `DEPENDENCY_NORMAL ('n')` — drop blocked unless CASCADE
- `DEPENDENCY_AUTO ('a')` — drop the depender automatically when the
  referenced object is dropped (e.g., index of a table)
- `DEPENDENCY_INTERNAL ('i')` — depender is internal, dropping the
  referenced means dropping us, but a direct DROP of us is forbidden
- `DEPENDENCY_EXTENSION ('e')` — depender belongs to a pg_extension
- `DEPENDENCY_AUTO_EXTENSION ('x')` — like AUTO but extension-owned
- `DEPENDENCY_PARTITION_PRI ('P')` / `_SEC ('S')` — partition links

### performDeletion  (importance 0.85, Tier 1)

**Signature** (`dependency.c`):
```c
void performDeletion(const ObjectAddress *object,
                     DropBehavior behavior, int flags);
```

**Logic**:

1. `AcquireDeletionLock(object, flags)` — lock the object so it cannot be
   modified concurrently. For relations this is `LockRelationOid`; for
   non-relation objects it is `LockSharedObject` or
   `LockDatabaseObject`.
2. `findDependentObjects()` — depth-first walk of pg_depend starting from
   the target. Builds an `ObjectAddresses` list of every object that must be
   dropped. Honors RESTRICT (abort if dependents exist) vs CASCADE.
3. `reportDependentObjects()` — if RESTRICT and dependents exist, emit a
   user-facing error listing them.
4. `deleteObjectsInList()` — call `deleteOneObject()` for each in the
   list, in dependency-respecting order.

`deleteOneObject` dispatches by classid: RelationRelationId →
`heap_drop_with_catalog`, ProcedureRelationId → `RemoveFunctionById`, etc.,
plus a final `deleteSharedDependencyRecordsFor` for shared depends.

### findDependentObjects

The recursive pg_depend walker. It uses
`object_address_present_add_flags` to detect cycles and to avoid revisiting
objects that have already been added to the deletion set. The walk is
DFS-on-pg_depend with a visited set; each pg_depend row that links our object
to something else either:

- triggers an error (NORMAL behavior with RESTRICT),
- adds the dependent to the to-delete list (AUTO, INTERNAL, AUTO_EXTENSION),
- gates the whole operation (INTERNAL: caller must drop the parent instead).

### AcquireDeletionLock

Picks the right lock manager method per classid. For relations, takes
`AccessExclusiveLock`. For non-relation objects (functions, types, namespaces),
takes `LockSharedObject` / `LockDatabaseObject` with appropriate level.

## namespace.c — schema resolution

### RangeVarGetRelid  (importance 0.85, Tier 1)

**Signature** (`namespace.c`):
```c
Oid RangeVarGetRelid(const RangeVar *relation,
                     LOCKMODE lockmode, bool missing_ok);
```

A wrapper around `RangeVarGetRelidExtended()` that resolves a possibly-qualified
name `(catalog).schema.relname` to an OID, taking the requested lockmode in
the process.

**Logic**:

1. If `relation->schemaname` is not NULL, look up that schema in pg_namespace
   and search only there.
2. Otherwise iterate `search_path` (cached in `activeSearchPath`).
3. For each candidate namespace, call `get_relname_relid` (a syscache lookup
   on `RELNAMENSP`).
4. As soon as a hit is found, take `lockmode` on the relation OID via
   `LockRelationOid`. If the relation has been dropped between the syscache
   lookup and the lock acquisition, retry from step 1 — this is the
   "lock-then-recheck-name" protocol.
5. Return the OID.

### Other namespace.c functions

| Function                         | Purpose                                        |
|----------------------------------|------------------------------------------------|
| `RangeVarGetCreationNamespace`   | resolve where a new relation should be created |
| `LookupExplicitNamespace`        | one-shot schema lookup (no search_path)        |
| `recomputeNamespacePath`         | refresh the search_path cache after SET        |
| `GetTempNamespaceProcNumber`     | which proc owns a temp namespace               |
| `isOtherTempNamespace`           | guard against accessing other backend's temps  |

## storage.c — the catalog/physical bridge

### RelationCreateStorage  (importance 0.85, Tier 1)

**Signature** (`storage.c:121`):
```c
SMgrRelation RelationCreateStorage(RelFileLocator rlocator, char relpersistence,
                                   bool register_delete);
```

**Logic**:

1. `smgropen(rlocator, INVALID_PROC_NUMBER)` to get the SMgrRelation handle.
2. `smgrcreate(srel, MAIN_FORKNUM, false)` to create the file.
3. If `register_delete`, call `pendingDeletesAdd(rlocator, true /*atCommit*/)`
   so abort will unlink the file.
4. Call `log_smgrcreate(rlocator, MAIN_FORKNUM)` to emit `XLOG_SMGR_CREATE`
   (only for permanent relations; unlogged + temp skip this).

```c
void log_smgrcreate(const RelFileLocator *rlocator, ForkNumber forkNum)
{
    xl_smgr_create xlrec;

    xlrec.rlocator = *rlocator;
    xlrec.forkNum = forkNum;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xlrec));
    XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
}
```

### RelationDropStorage  (importance 0.78)

`storage.c`:
```c
void RelationDropStorage(Relation rel);
```

Adds the relfilelocator to the per-transaction `pendingDeletes` list with
`atCommit = true`. The actual `smgrunlink` happens in `smgrDoPendingDeletes`,
called from `RecordTransactionCommit` (commit) or `AbortTransaction` (abort).

### smgrDoPendingDeletes  (importance 0.78)

Runs through the `pendingDeletes` list. On commit, executes every
`atCommit = true` unlink and discards every `atCommit = false` (those are
"undo" entries for newly-created files we now want to keep). On abort, does
the inverse.

`xact_redo_commit` calls a stripped-down version (`smgrDoPendingDeletes(true)`)
to honor the dropped-relfilelocator list embedded in `xl_xact_commit`. This
makes file unlink crash-safe and replication-safe.

### RelationTruncate

Emits `XLOG_SMGR_TRUNCATE` and calls `smgrtruncate` on every fork. The redo
function additionally calls `visibilitymap_prepare_truncate` and
`FreeSpaceMapPrepareTruncateRel`, so VM/FSM forks shrink in lockstep with the
heap fork.

## toasting.c

### NewHeapCreateToastTable  (importance 0.7)

`toasting.c`:
```c
void NewHeapCreateToastTable(Oid relOid, Datum reloptions, LOCKMODE lockmode,
                             Oid OIDOldToast);
```

Creates a TOAST sidecar table when needed (relation has variable-length
columns large enough to require out-of-line storage). Calls
`heap_create_with_catalog` to make the toast table, `index_create` to make its
PK index, and `recordDependencyOn(toast_rel, main_rel, DEPENDENCY_INTERNAL)`.

## aclchk.c

### ExecGrantStmt_oids

Translates a `GrantStmt` into pg_class / pg_namespace / pg_proc / etc.
ACL updates. For each referenced object, it:

1. Fetches the current ACL via syscache.
2. Calls `aclupdate()` to compute the new ACL.
3. Calls `CatalogTupleUpdate` to write the new tuple.
4. Calls `recordDependencyOnNewAcl` to note any role dependencies.

### pg_class_aclmask / pg_namespace_aclmask / etc.

`aclmask` family functions. Cheap path: ACL hash table cached in syscache;
expensive path on miss: scan pg_class.

## objectaddress.c

### get_object_address

Translates a parser-level `ObjectWithArgs` (which describes one DDL target,
e.g. `FUNCTION foo(int)`) into an `ObjectAddress {classId, objectId, subId}`.
This is the canonical bridge from SQL syntax to internal object identity.

The class table `ObjectProperty[]` (objectaddress.c) drives every classId-keyed
operation: which catalog holds the object, which OID column to look up, which
syscache to use, which name column reports the object's name, etc.

## objectaccess.c

Provides the `object_access_hook` extension point. Every catalog-mutating
function calls `InvokeObjectPostCreateHook` / `_PostAlterHook` / `_DropHook` so
sepgsql, pg_audit, and similar extensions can react to DDL.

## partition.c & pg_inherits.c

`partition.c` reads pg_partitioned_table rows and builds in-memory
`PartitionDesc` and `PartitionKey`. `pg_inherits.c` exports
`StoreSingleInheritance`, `find_inheritance_children`, `find_all_inheritors`.
Both are reached through the catalog cache stack rather than direct
catalog scans.

## The "in-place update" exception

For pg_class.relfrozenxid (and similar), an ordinary MVCC update would
generate dead row versions every time vacuum advances the freeze horizon.
The cure: `heap_inplace_update_and_unlock` overwrites the tuple's payload
without producing a new tuple version. This is safe only for
`IsInplaceUpdateRelation(rel)` — currently pg_class and pg_database — and
only for fields whose visibility every viewer is willing to ignore (the
freeze horizon is only a hint to vacuum scheduling).

`heap_inplace_update_and_unlock` emits a small `xl_heap_inplace` WAL
record so the in-place change is durable and replayed on standbys.

## Cross-references

- `component_catalog_caches.md` — what `CatalogTupleInsert` invalidates.
- `component_cache_invalidation.md` — `CacheInvalidateHeapTuple` deep dive.
- `component_relmapper.md` — what mapped catalogs do at storage-bridge time.
- `component_persistence_and_wal_records.md` — `XLOG_SMGR_CREATE`, `XLOG_SMGR_TRUNCATE`.

## Source references

- `src/backend/catalog/indexing.c:233` — CatalogTupleInsert
- `src/backend/catalog/indexing.c:313` — CatalogTupleUpdate
- `src/backend/catalog/indexing.c:365` — CatalogTupleDelete
- `src/backend/catalog/heap.c:290` — heap_create
- `src/backend/catalog/heap.c:821` — AddNewAttributeTuples
- `src/backend/catalog/heap.c:896` — InsertPgClassTuple
- `src/backend/catalog/heap.c:969` — AddNewRelationTuple
- `src/backend/catalog/heap.c:1105` — heap_create_with_catalog
- `src/backend/catalog/heap.c:1767` — heap_drop_with_catalog
- `src/backend/catalog/index.c` — index_create, index_drop, ...
- `src/backend/catalog/dependency.c` — recordDependencyOn, performDeletion, findDependentObjects
- `src/backend/catalog/namespace.c` — RangeVarGetRelid
- `src/backend/catalog/storage.c:121` — RelationCreateStorage
- `src/backend/catalog/storage.c:186` — log_smgrcreate
- `src/backend/catalog/toasting.c` — NewHeapCreateToastTable
- `src/backend/catalog/aclchk.c` — ExecGrantStmt_oids
- `src/backend/catalog/objectaddress.c` — get_object_address, ObjectProperty[]
- `src/backend/catalog/objectaccess.c` — object_access_hook
