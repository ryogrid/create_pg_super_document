# EventCacheLookup

## Location
src/backend/utils/cache/evtcache.c: 63 - 76

## Overview
Searches the event trigger cache by trigger event type and returns a list of matching event trigger cache items.

## Definition
```c
List *EventCacheLookup(EventTriggerEvent event)
```

## Detailed Description
EventCacheLookup is a cache lookup function that retrieves event trigger configurations for a specific event type from the PostgreSQL event trigger cache. The function first ensures the cache is valid by calling BuildEventTriggerCache() if needed, then performs a hash table lookup to find the appropriate trigger list for the given event. This is part of PostgreSQL's event trigger system that allows users to define triggers that fire on DDL commands and other database events.

The function includes an important warning that callers should copy any data they want to preserve across operations that might touch system catalogs, as cache resets could invalidate the returned data.

## Parameters / Member Variables
- `event`: An EventTriggerEvent enum value specifying the type of event to look up (EVT_DDLCommandStart, EVT_DDLCommandEnd, EVT_SQLDrop, EVT_TableRewrite, or EVT_Login)

## Dependencies
- Functions called/Symbols referenced:
  - BuildEventTriggerCache
  - hash_search
  - HASH_FIND
  - ETCS_VALID
- Called from (representative examples):
  - EventTriggerCommonSetup (src/backend/commands/event_trigger.c:680)
  - trackDroppedObjectsNeeded (src/backend/commands/event_trigger.c:1252-1254)

## Notes and Other Information
- Returns NIL (empty list) if no triggers are found for the specified event type
- The returned list contains EventTriggerCacheItem structures with function OIDs, enabled status, and command tag sets
- Cache validity is automatically checked and rebuilt if necessary using the EventTriggerCacheState global variable
- Part of the event trigger subsystem located in src/backend/utils/cache/evtcache.c
- Memory returned should be treated as volatile and copied if persistence across catalog operations is required