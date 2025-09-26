# EventTriggerCacheItem

## Location
[src/include/utils/evtcache.h:34-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/evtcache.h#L34-L38)

## Overview
EventTriggerCacheItem represents a cached entry for an event trigger in PostgreSQL's event trigger cache system, storing the essential information needed to determine when and how to execute an event trigger function.

## Definition

```c
typedef struct
{
	Oid			fnoid;			/* function to be called */
	char		enabled;		/* as SESSION_REPLICATION_ROLE_* */
	Bitmapset  *tagset;			/* command tags, or NULL if empty */
} EventTriggerCacheItem;
```
## Detailed Description
EventTriggerCacheItem is a core data structure used in PostgreSQL's event trigger caching mechanism. It stores the metadata necessary to efficiently determine which event trigger functions should be executed for a given event. The structure is used internally by the event trigger cache system (evtcache.c) to maintain fast access to event trigger information without repeatedly querying the system catalogs.

When PostgreSQL builds the event trigger cache, it reads from the pg_event_trigger system catalog and creates EventTriggerCacheItem instances for each enabled event trigger. These items are then organized into lists associated with specific events (DDL command start/end, SQL drop, table rewrite, login) in the EventTriggerCache hash table.

The structure supports filtering based on session replication roles and command tags, allowing PostgreSQL to quickly determine which triggers are applicable for a given operation without expensive catalog lookups during command execution.

## Parameters / Member Variables
- : The OID of the event trigger function that should be called when this trigger fires. This references a function in pg_proc that contains the actual trigger logic.
- : Controls when the trigger fires based on the current session replication role. Uses the same values as SESSION_REPLICATION_ROLE_* constants (TRIGGER_FIRES_ON_ORIGIN, TRIGGER_FIRES_ON_REPLICA, etc.).
- : A bitmapset containing the command tags that this event trigger should respond to. If NULL or empty, the trigger fires for all commands of the associated event type. Otherwise, it only fires for commands whose tags are present in this set.

## Dependencies
- Functions called/Symbols referenced:
  - EventCacheLookup (function that retrieves lists of these items)
  - EventTriggerEvent (enum defining event types)
- Called from (representative examples):
  - filter_event_trigger (filters items based on replication role and tags)
  - EventTriggerCommonSetup (processes items to build trigger execution lists)
  - BuildEventTriggerCache (creates and populates items from pg_event_trigger catalog)

## Notes and Other Information
- EventTriggerCacheItem instances are allocated in the EventTriggerCacheContext memory context and persist until the cache is rebuilt
- The cache is invalidated and rebuilt when changes occur to the pg_event_trigger catalog
- The tagset field uses PostgreSQL's efficient bitmapset data structure to store command tag membership information
- Items are stored in lists within EventTriggerCacheEntry structures, which are keyed by EventTriggerEvent in the main cache hash table
- Disabled triggers (evtenabled == TRIGGER_DISABLED) are filtered out during cache construction and never create EventTriggerCacheItem instances
- The structure is designed for efficient filtering during trigger execution to minimize performance impact on normal SQL operations