# Component: Hooks and Extensibility (around metadata)

[Top: ../README.md](../../README.md)

## Overview

PostgreSQL exposes several hook points around metadata operations so that
extensions like sepgsql, pg_audit, and custom plug-ins can react to catalog
changes without modifying core code. This document inventories those hooks
and how they tie to the metadata flow.

## object_access_hook

`src/include/catalog/objectaccess.h`:
```c
typedef enum ObjectAccessType
{
    OAT_POST_CREATE,
    OAT_DROP,
    OAT_POST_ALTER,
    OAT_NAMESPACE_SEARCH,
    OAT_FUNCTION_EXECUTE,
    OAT_TRUNCATE,
} ObjectAccessType;

typedef void (*object_access_hook_type)(ObjectAccessType access,
                                        Oid classId, Oid objectId,
                                        int subId, void *arg);
extern PGDLLIMPORT object_access_hook_type object_access_hook;
```

Catalog-mutator helper functions emit calls via:

```c
InvokeObjectPostCreateHook(classId, objectId, subId);
InvokeObjectDropHook       (classId, objectId, subId);
InvokeObjectPostAlterHook  (classId, objectId, subId);
InvokeNamespaceSearchHook  (namespaceOid, ereport_on_violation);
InvokeFunctionExecuteHook  (functionOid);
```

These macros expand to:
```c
if (object_access_hook) (*object_access_hook)(OAT_POST_CREATE, classId, objectId, subId, NULL);
```

So the hook is a no-op unless a loaded module installs a function.

### sepgsql

`contrib/sepgsql/` installs `object_access_hook` to consult SELinux
policy on every catalog operation. For example, on `OAT_POST_CREATE`
of a relation, sepgsql checks whether the current security label allows
the new object's namespace.

### pg_audit

`contrib/pgaudit/` (and its third-party variants) hooks at OAT_POST_CREATE,
OAT_DROP, OAT_POST_ALTER to write audit events.

## Catcache callback registry

```c
void CacheRegisterSyscacheCallback(int cacheid,
                                   SyscacheCallbackFunction func,
                                   Datum arg);
```

Limit: `MAX_SYSCACHE_CALLBACKS = 64` (compile-time).

Used internally by:
- `plancache.c` registers RELOID, NAMESPACEOID, OPEROID, AMOID, FOREIGNDATAWRAPPEROID, FOREIGNSERVEROID, USERMAPPINGOID, ... callbacks → invalidates cached plans when those caches change.
- `partcache.c` registers RELOID callback → invalidate PartitionDesc.
- `typcache.c` registers TYPEOID callback → invalidate TypeCacheEntry.
- `relfilenumbermap.c` registers RELOID callback → invalidate the
  relfilenode→OID reverse map.

Extension example: pg_stat_statements registers RELOID + NAMESPACEOID
callbacks to invalidate its plan-text cache.

## Relcache callback registry

```c
void CacheRegisterRelcacheCallback(RelcacheCallbackFunction func, Datum arg);
```

Limit: `MAX_RELCACHE_CALLBACKS = 64`.

The callback is invoked for every relcache invalidation. The function
receives the affected relid (or InvalidOid for "all").

Used by:
- `plancache.c` — invalidate plans.
- `pg_publication.c` — invalidate publication caches when pg_publication_rel changes.
- many built-in caches.

## CallSyscacheCallbacks / CallRelcacheCallbacks

Internal dispatchers; `LocalExecuteInvalidationMessage` calls
`CallSyscacheCallbacks(cacheid, hashvalue)` after invalidating the
catcache. Extensions typically do NOT call these directly.

## custom_rmgr — extension WAL records

`src/include/access/xlog_internal.h` exports `RegisterCustomRmgr` which lets
extensions allocate a private RM_* ID:

```c
typedef struct RmgrData {
    const char *rm_name;
    void (*rm_redo)(XLogReaderState *record);
    void (*rm_desc)(StringInfo buf, XLogReaderState *record);
    const char *(*rm_identify)(uint8 info);
    void (*rm_startup)(void);
    void (*rm_cleanup)(void);
    bool (*rm_mask)(char *pagedata, BlockNumber blkno);
    void (*rm_decode)(...);
} RmgrData;

extern void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);
```

The reserved range `RM_EXPERIMENTAL_ID..RM_MAX_ID` is for extensions
(currently 128..255). pg_logical does not use this; pgrowlocks does not.
This hook is rare in practice but useful for index AMs (e.g., bloom).

## CacheRegisterSyscacheCallback caveats

1. **No allocations**: callbacks may be invoked with locks held; allocating
   memory is unsafe.
2. **No catalog access**: the callback fires while the inval system is
   active; calling SearchSysCache could recurse.
3. **Side-effect free**: the callback should mark its own cache stale, not
   actually rebuild it (rebuild can happen lazily later).

## Other extension-facing surfaces around metadata

| Hook                    | Where                                | What it does                                  |
|-------------------------|--------------------------------------|-----------------------------------------------|
| `process_utility_hook`  | utility.c                            | wrap DDL execution                            |
| `ProcessUtility_hook`   | utility.c                            | same (alias)                                  |
| `planner_hook`          | planner.c                            | swap in a custom planner                      |
| `ExecutorStart_hook`    | execMain.c                           | wrap executor start                           |
| `ExplainOneQuery_hook`  | explain.c                            | wrap EXPLAIN                                  |
| `emit_log_hook`         | elog.c                               | redirect log output                           |
| `shmem_request_hook`    | startup.c                            | request shared memory at preload time         |
| `shmem_startup_hook`    | startup.c                            | initialize shared memory at startup           |
| `ClientAuthentication_hook` | auth.c                           | custom auth                                   |
| `needs_fmgr_hook`       | fmgr.c                               | per-function call hook                        |

These do not directly modify metadata, but interact with it via the public
catalog cache APIs.

## Common patterns

### Track catalog row creation in an extension

```c
static object_access_hook_type prev_hook = NULL;

static void my_hook(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg)
{
    if (prev_hook) prev_hook(access, classId, objectId, subId, arg);
    if (access == OAT_POST_CREATE && classId == RelationRelationId) {
        elog(LOG, "table %u was created", objectId);
    }
}

void _PG_init(void)
{
    prev_hook = object_access_hook;
    object_access_hook = my_hook;
}
```

### Invalidate a per-query cache when pg_class changes

```c
static void my_relcache_callback(Datum arg, Oid relid)
{
    /* Invalidate our cached metadata for relid */
}

void _PG_init(void)
{
    CacheRegisterRelcacheCallback(my_relcache_callback, (Datum) 0);
}
```

## Cross-references

- `component_catalog_modification_apis.md` — where InvokeObject*Hook calls
  live.
- `component_cache_invalidation.md` — Call*Callbacks dispatch.
- `component_persistence_and_wal_records.md` — custom_rmgr in rmgrlist.

## Source references

- `src/include/catalog/objectaccess.h` — ObjectAccessType, hook macros
- `src/backend/catalog/objectaccess.c` — hook framework
- `src/backend/utils/cache/inval.c::CacheRegisterSyscacheCallback`
- `src/backend/utils/cache/inval.c::CacheRegisterRelcacheCallback`
- `src/backend/utils/cache/inval.c::CallSyscacheCallbacks`
- `src/backend/utils/cache/inval.c::CallRelcacheCallbacks`
- `src/backend/access/transam/xlog.c::RegisterCustomRmgr`
- `src/include/access/xlog_internal.h` — RmgrData
