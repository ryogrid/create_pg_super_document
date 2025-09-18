# get_all_vacuum_rels

## Location
[src/backend/commands/vacuum.c:1021-1082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1021-L1082)

## Overview
Constructs a list of VacuumRelations for all vacuumable relations in the current database by scanning the system catalog.

## Definition
```c
static List *
get_all_vacuum_rels(MemoryContext vac_context, int options)
```

## Detailed Description
The get_all_vacuum_rels function performs a full catalog scan of pg_class to identify all relations that can be vacuumed or analyzed in the current database. It filters relations based on their kind (regular tables, materialized views, and partitioned tables) and checks permissions for each relation before adding it to the result list.

The function creates VacuumRelation entries with OIDs but no RangeVar, since these are discovered relations rather than user-specified ones. This approach avoids inappropriate error messages if a relation becomes unavailable later. All memory allocation is performed in the provided vac_context.

## Parameters / Member Variables
- `vac_context`: Memory context in which to allocate new VacuumRelation structures
- `options`: Vacuum options flags used for permission checking

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - vacuum_is_permitted_for_relation
  - [makeVacuumRelation](../m/makeVacuumRelation.md)
  - [table_endscan](../t/table_endscan.md)
  - table_close
- Called from (representative examples):
  - vacuum (src/backend/commands/vacuum.c:547)

## Notes and Other Information
- Includes partitioned tables in the scan; caller decides whether to process them
- Filters by relation kind: regular tables, materialized views, and partitioned tables only
- Performs permission checking for each relation before inclusion
- Uses catalog scan with AccessShareLock for safe concurrent access
- Creates VacuumRelation entries with OIDs only (no RangeVar or column lists)
- Location: src/backend/commands/vacuum.c:1021-1082