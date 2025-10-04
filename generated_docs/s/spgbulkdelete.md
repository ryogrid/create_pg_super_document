# spgbulkdelete

## Location
[src/backend/access/spgist/spgvacuum.c:916-935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L916-L935)

## Overview
Entry point function for SP-GiST bulk delete operations, setting up the bulk delete state and initiating the vacuum scan.

## Definition

```c
struct */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
```
## Detailed Description
The `spgbulkdelete` function serves as the main entry point for SP-GiST bulk deletion operations during VACUUM commands. It follows the standard PostgreSQL index AM interface for bulk delete operations.

The function performs minimal setup work:
1. Allocates a new statistics structure if this is the first call, or reuses an existing one for subsequent calls within the same VACUUM command
2. Initializes the bulk delete state structure with all necessary context information
3. Delegates the actual work to `spgvacuumscan`

This design allows for multiple bulk delete passes within a single VACUUM operation while maintaining state continuity through the shared statistics structure.

## Parameters / Member Variables
- `info`: Index vacuum information containing the index relation, heap relation, and buffer access strategy
- `stats`: Existing statistics structure to reuse, or NULL to allocate a new one
- `callback`: Function to call for each heap tuple to determine if it should be deleted
- `callback_state`: Opaque state data passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [spgvacuumscan](spgvacuumscan.md)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md)
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md)
  - IndexBulkDeleteCallback
- Called from (representative examples):
  - [spghandler](spghandler.md) (via function pointer in index AM handler)

## Notes and Other Information
- Implements the standard PostgreSQL index access method interface for bulk delete
- Supports incremental statistics accumulation across multiple calls within the same VACUUM
- Returns allocated statistics structure that must be freed by caller
- Part of the SP-GiST access method's integration with PostgreSQL's vacuum system
- The callback mechanism allows VACUUM to determine which tuples should be deleted based on heap tuple visibility

## Simplified Source

```c
IndexBulkDeleteResult *
spgbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
              IndexBulkDeleteCallback callback, void *callback_state)
{
    spgBulkDeleteState bds;

    // Allocate stats structure if first time, otherwise reuse existing
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    // Initialize bulk delete state
    bds.info = info;
    bds.stats = stats;
    bds.callback = callback;
    bds.callback_state = callback_state;

    // Perform the actual vacuum scan
    spgvacuumscan(&bds);

    return stats;
}
```