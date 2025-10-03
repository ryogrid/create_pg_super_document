# prepare_projection_slot

## Location
[src/backend/executor/nodeAgg.c:1249-1293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1249-L1293)

## Overview
Prepares a tuple slot for finalization and projection by nullifying attributes that should be read as null in the context of the current grouping set and setting up grouping expression context.

## Definition
```c
static void prepare_projection_slot(AggState *aggstate, TupleTableSlot *slot, int currentSet)
```

## Detailed Description
This function modifies a tuple slot in preparation for result projection based on the current grouping set. It handles the complex logic of GROUPING SETS by forcing certain attributes to null when they are not part of the current grouping set. The function operates directly on the slot's internal arrays, which is considered somewhat hacky but was deemed the best approach. It handles three key scenarios: empty input tuples (forces all values to NULL), regular tuples with grouping sets (selectively nullifies non-grouped columns), and sets up the grouped_cols bitmap for GroupingExpr evaluation.

## Parameters / Member Variables
- `aggstate`: The aggregate execution state containing grouping set information
- `slot`: The tuple slot to be prepared for projection
- `currentSet`: Index of the current grouping set being processed

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY
  - [ExecStoreAllNullTuple](../E/ExecStoreAllNullTuple.md)
  - [slot_getsomeattrs](../s/slot_getsomeattrs.md)
  - linitial_int
  - lfirst_int
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_retrieve_hash_table_in_memory](../a/agg_retrieve_hash_table_in_memory.md)

## Notes and Other Information
- Relies on the assumption that nothing will extract the whole tuple from the slot, only reference individual attributes
- System columns are assumed to not need nullification as they are projected in the outer plan target list
- Within a phase, attribute values don't need to be recovered once set to null
- The all_grouped_cols list is arranged in descending order for optimization
- Critical for implementing SQL GROUPING SETS functionality where different grouping combinations produce different null patterns
- The direct slot manipulation is acknowledged as somewhat ugly but was chosen over alternatives for performance reasons

## Simplified Source

```c
static void prepare_projection_slot(AggState *aggstate, TupleTableSlot *slot, int currentSet) {
    if (aggstate->phase->grouped_cols) {
        Bitmapset *grouped_cols = aggstate->phase->grouped_cols[currentSet];

        aggstate->grouped_cols = grouped_cols;

        if (TTS_EMPTY(slot)) {
            // Force all values to NULL for empty grouping set
            ExecStoreAllNullTuple(slot);
        } else if (aggstate->all_grouped_cols) {
            ListCell *lc;

            // Ensure all needed attributes are materialized
            slot_getsomeattrs(slot, linitial_int(aggstate->all_grouped_cols));

            // Nullify attributes not in current grouping set
            foreach(lc, aggstate->all_grouped_cols) {
                int attnum = lfirst_int(lc);

                if (!bms_is_member(attnum, grouped_cols))
                    slot->tts_isnull[attnum - 1] = true;
            }
        }
    }
}
```