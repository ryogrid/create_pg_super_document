# update_relstats_all_indexes

## Location
[src/backend/access/heap/vacuumlazy.c:3071-3105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L3071-L3105)

## Overview
`update_relstats_all_indexes` updates index statistics in the pg_class system catalog after a vacuum operation, but only when the collected statistics are accurate (non-estimated).

## Definition
```c
static void update_relstats_all_indexes(LVRelState *vacrel)
```

## Detailed Description
This function iterates through all indexes associated with a relation that underwent vacuum processing and updates their statistics in the pg_class system catalog. It only updates statistics when they are considered accurate, which means the statistics were gathered through actual counting rather than estimation. For each qualifying index, it calls vac_update_relstats with the collected page count and tuple count information. The function is designed to be called at the end of vacuum operations when index cleanup has been performed and reliable statistics have been gathered.

The function skips indexes where no statistics were collected (NULL) or where the statistics are based on estimates rather than exact counts, ensuring that only reliable data is stored in the system catalog.

## Parameters / Member Variables
- `vacrel`: Pointer to LVRelState structure containing vacuum operation state, index relations, and collected statistics

## Dependencies
- Functions called/Symbols referenced:
  - [vac_update_relstats](../v/vac_update_relstats.md)
  - InvalidTransactionId
  - InvalidMultiXactId
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)

## Notes and Other Information
- This is a static function, only accessible within vacuumlazy.c
- The function requires that index cleanup was performed (asserted via `vacrel->do_index_cleanup`)
- Only processes indexes with non-NULL statistics that are not based on estimates
- Calls vac_update_relstats with specific parameters: num_pages, num_index_tuples, hasindex=0, isvacuum=false, InvalidTransactionId, InvalidMultiXactId, and NULL for additional parameters
- The statistics update helps the query planner make better decisions by providing accurate index size and tuple count information
- Updates are atomic per index but the overall operation processes all indexes sequentially

## Simplified Source

```c
static void update_relstats_all_indexes(LVRelState *vacrel) {
    // Update statistics for all indexes with accurate (non-estimated) data
    for (int idx = 0; idx < vacrel->nindexes; idx++) {
        Relation index = vacrel->indrels[idx];
        IndexBulkDeleteResult *stats = vacrel->indstats[idx];

        // Skip indexes with no stats or estimated counts
        if (stats == NULL || stats->estimated_count)
            continue;

        // Update pg_class with accurate index statistics
        vac_update_relstats(index, stats->num_pages, stats->num_index_tuples,
                           0, false, InvalidTransactionId, InvalidMultiXactId,
                           NULL, NULL, false);
    }
}
```