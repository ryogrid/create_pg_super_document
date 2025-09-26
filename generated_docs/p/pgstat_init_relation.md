# pgstat_init_relation

## Location
[src/backend/utils/activity/pgstat_relation.c:92-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L92-L131)

## Overview
Initializes a relation cache entry to enable statistics tracking whenever a relation is opened, setting up the basic framework for collecting access statistics.

## Definition
```c
void pgstat_init_relation(Relation rel)
```

## Detailed Description
This function is called whenever a relation is opened to initialize the statistics tracking capabilities for that relation. It determines whether statistics should be collected for the relation based on its type and the current configuration settings.

The function performs several key checks: it only enables statistics for relations that have storage (tables, indexes, etc.) and partitioned tables, as other relation types don't require statistics tracking. It also respects the global `pgstat_track_counts` setting - if statistics tracking is disabled globally, it ensures any existing statistics associations are cleaned up and disables tracking for the relation.

The function assumes that the relation's `pgstat_info` field has been zeroed during relcache entry creation and treats it as long-lived data. This function only sets up the basic enablement flag; actual memory allocation and shared memory references are handled later by `pgstat_assoc_relation()`.

## Parameters / Member Variables
- `rel`: The Relation object being opened that needs statistics tracking initialization

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_HAS_STORAGE (macro)
  - [pgstat_unlink_relation](pgstat_unlink_relation.md)
  - pgstat_track_counts (global variable)
  - RELKIND_PARTITIONED_TABLE (constant)
- Called from (representative examples):
  - [relation_open](../r/relation_open.md)
  - [try_relation_open](../t/try_relation_open.md)

## Notes and Other Information
- This function is called every time a relation is opened, making it a critical performance path
- The function only sets up the framework for statistics tracking but does not create actual shared memory references
- Statistics are only tracked for relations with storage and partitioned tables; other relation kinds are excluded
- The function respects the global pgstat_track_counts setting and can disable tracking dynamically
- If statistics tracking is disabled after being enabled, the function properly cleans up existing associations
- The actual allocation of pending stats memory and shared memory references happens in pgstat_assoc_relation()