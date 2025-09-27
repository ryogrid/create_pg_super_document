# BuildEventTriggerCache

## Location
[src/backend/utils/cache/evtcache.c:77-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/evtcache.c#L77-L221)

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

## Simplified Source

```c
// Simplified version of BuildEventTriggerCache
static void BuildEventTriggerCache(void) {
    HASHCTL ctl;
    HTAB *cache;
    MemoryContext oldcontext;
    Relation rel, irel;
    SysScanDesc scan;

    // Step 1: Initialize or reset memory context
    if (EventTriggerCacheContext != NULL) {
        // Reset existing context to clean up memory
        MemoryContextReset(EventTriggerCacheContext);
    } else {
        // First time: create context and register invalidation callback
        if (CacheMemoryContext == NULL)
            CreateCacheMemoryContext();
        EventTriggerCacheContext = AllocSetContextCreate(CacheMemoryContext,
                                                        "EventTriggerCache",
                                                        ALLOCSET_DEFAULT_SIZES);
        CacheRegisterSyscacheCallback(EVENTTRIGGEROID,
                                    InvalidateEventCacheCallback,
                                    (Datum) 0);
    }

    // Step 2: Switch to cache memory context and mark rebuild in progress
    oldcontext = MemoryContextSwitchTo(EventTriggerCacheContext);
    EventTriggerCacheState = ETCS_REBUILD_STARTED;

    // Step 3: Create new hash table for event triggers
    ctl.keysize = sizeof(EventTriggerEvent);
    ctl.entrysize = sizeof(EventTriggerCacheEntry);
    ctl.hcxt = EventTriggerCacheContext;
    cache = hash_create("EventTriggerCacheHash", 32, &ctl,
                       HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    // Step 4: Open pg_event_trigger table and its name index for ordered scan
    rel = relation_open(EventTriggerRelationId, AccessShareLock);
    irel = index_open(EventTriggerNameIndexId, AccessShareLock);
    scan = systable_beginscan_ordered(rel, irel, NULL, 0, NULL);

    // Step 5: Process each event trigger tuple
    for (;;) {
        HeapTuple tup;
        Form_pg_event_trigger form;
        char *evtevent;
        EventTriggerEvent event;
        EventTriggerCacheItem *item;
        EventTriggerCacheEntry *entry;
        bool found;

        // Get next tuple from ordered scan
        tup = systable_getnext_ordered(scan, ForwardScanDirection);
        if (!HeapTupleIsValid(tup))
            break;

        // Skip disabled triggers
        form = (Form_pg_event_trigger) GETSTRUCT(tup);
        if (form->evtenabled == TRIGGER_DISABLED)
            continue;

        // Map event name string to event enum
        evtevent = NameStr(form->evtevent);
        if (strcmp(evtevent, "ddl_command_start") == 0)
            event = EVT_DDLCommandStart;
        else if (strcmp(evtevent, "ddl_command_end") == 0)
            event = EVT_DDLCommandEnd;
        else if (strcmp(evtevent, "sql_drop") == 0)
            event = EVT_SQLDrop;
        else if (strcmp(evtevent, "table_rewrite") == 0)
            event = EVT_TableRewrite;
        else if (strcmp(evtevent, "login") == 0)
            event = EVT_Login;
        else
            continue; // Unknown event type

        // Create cache item for this trigger
        item = palloc0(sizeof(EventTriggerCacheItem));
        item->fnoid = form->evtfoid;
        item->enabled = form->evtenabled;

        // Process tag array if present
        Datum evttags = heap_getattr(tup, Anum_pg_event_trigger_evttags,
                                   RelationGetDescr(rel), &evttags_isnull);
        if (!evttags_isnull)
            item->tagset = DecodeTextArrayToBitmapset(evttags);

        // Add item to appropriate cache entry
        entry = hash_search(cache, &event, HASH_ENTER, &found);
        if (found)
            entry->triggerlist = lappend(entry->triggerlist, item);
        else
            entry->triggerlist = list_make1(item);
    }

    // Step 6: Clean up scan resources
    systable_endscan_ordered(scan);
    index_close(irel, AccessShareLock);
    relation_close(rel, AccessShareLock);

    // Step 7: Install new cache and update state
    MemoryContextSwitchTo(oldcontext);
    EventTriggerCache = cache;

    // Mark cache as valid (unless invalidated during rebuild)
    if (EventTriggerCacheState == ETCS_REBUILD_STARTED)
        EventTriggerCacheState = ETCS_VALID;
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the main phases
- Simplified variable declarations for better readability
- Consolidated related operations into logical groups
- Removed detailed comments about edge cases and focused on main flow
- Made the event type mapping more readable with consistent formatting
- Clarified the memory context management pattern
- Emphasized the ordered scan approach and its purpose