# BuildEventTriggerCache

## Location
src/backend/utils/cache/evtcache.c: 77 - 221

## Overview
Rebuilds the event trigger cache by scanning the pg_event_trigger system catalog and constructing a hash table indexed by event type for efficient event trigger lookup.

## Definition
```c
static void BuildEventTriggerCache(void)
```

## Detailed Description
BuildEventTriggerCache is a static function responsible for constructing or rebuilding PostgreSQL's event trigger cache. The function performs a complete scan of the pg_event_trigger system catalog table, processes each enabled trigger record, and builds a hash table organized by event type for efficient lookup during event trigger execution. The cache building process includes memory context management, invalidation callback registration, and careful state tracking to handle concurrent invalidations during cache construction.

The function implements a robust caching strategy that handles both initial cache creation and subsequent rebuilds due to invalidation events. It uses a dedicated memory context (EventTriggerCacheContext) to isolate cache memory and includes logic to handle invalidations that occur during the rebuild process.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md), CreateCacheMemoryContext, AllocSetContextCreate
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md), InvalidateEventCacheCallback
  - [hash_create](../h/hash_create.md), hash_search
  - [relation_open](../r/relation_open.md), index_open, systable_beginscan_ordered, systable_getnext_ordered, systable_endscan_ordered
  - [index_close](../i/index_close.md), relation_close
  - [heap_getattr](../h/heap_getattr.md), DecodeTextArrayToBitmapset
  - [palloc0](../p/palloc0.md), lappend, list_make1
- Called from (representative examples):
  - [EventCacheLookup](../E/EventCacheLookup.md) (src/backend/utils/cache/evtcache.c:68)

## Notes and Other Information
- Creates a hash table with EventTriggerEvent as key and EventTriggerCacheEntry as value
- Handles five event types: ddl_command_start, ddl_command_end, sql_drop, table_rewrite, and login
- Uses ordered scanning on EventTriggerNameIndexId for deterministic cache building
- Implements state tracking (ETCS_REBUILD_STARTED, ETCS_VALID) to handle concurrent invalidations
- Skips disabled triggers (TRIGGER_DISABLED) during cache construction
- Processes event tag arrays using DecodeTextArrayToBitmapset for command filtering
- Memory context switching ensures all cache data is allocated in EventTriggerCacheContext
- Registers InvalidateEventCacheCallback for automatic cache invalidation on system catalog changes
- Robust against invalidation events occurring during cache reconstruction