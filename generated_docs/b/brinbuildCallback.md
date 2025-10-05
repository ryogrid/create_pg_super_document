# brinbuildCallback

## Location
[src/backend/access/brin/brin.c:985-1035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L985-L1035)

## Overview
Per-heap-tuple callback function used during BRIN index build that processes individual tuples and accumulates their values into summary data for page ranges.

## Definition

```c
static void
brinbuildCallback(Relation index,
				  ItemPointer tid,
				  Datum *values,
				  bool *isnull,
				  bool tupleIsAlive,
				  void *brstate)
```
## Detailed Description
The brinbuildCallback function is called for each heap tuple during BRIN index construction via table_index_build_scan. It maintains the core logic for building BRIN summary information by:

1. Determining which page range the current tuple belongs to based on its TID
2. Completing and inserting index tuples for ranges that have been fully processed  
3. Advancing to new ranges when the current tuple is beyond the current range boundary
4. Accumulating the current tuple's values into the running summary for its range

When transitioning between ranges, the function:
- Creates and inserts the completed index tuple for the previous range
- Advances the current range start position
- Re-initializes the summary tuple for the new range
- Handles cases where pages were skipped (due to no live tuples) by ensuring index tuples are created for those ranges too

The function processes tuples sequentially as they are encountered during the table scan, building up summary statistics for each page range.

## Parameters / Member Variables
- `index`: The BRIN index relation being built
- `tid`: ItemPointer (TID) of the current heap tuple being processed
- `*values`: Array of Datum values for the indexed attributes of the current tuple
- `*isnull`: Array indicating which values are NULL
- `tupleIsAlive`: Boolean indicating if the tuple is visible (not used in current implementation)
- `*brstate`: BrinBuildState structure containing build state information
## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md): Extracts block number from tuple ID
  - [form_and_insert_tuple](../f/form_and_insert_tuple.md): Creates and inserts completed BRIN index tuple
  - [brin_memtuple_initialize](brin_memtuple_initialize.md): Reinitializes summary tuple for new range
  - [add_values_to_range](../a/add_values_to_range.md): Accumulates tuple values into range summary
  - BRIN_elog: Debug logging for range completion
- Called from (representative examples):
  - [brinbuild](brinbuild.md): Main BRIN index build function
  - [summarize_range](../s/summarize_range.md): Function to summarize a specific page range

## Notes and Other Information
- This is a static function, only used internally within the BRIN module
- The function handles the case where many pages might be skipped during scanning
- Does not insert the final range's index tuple - caller must handle this separately
- Uses a while loop to handle cases where multiple ranges are completed in one call
- Debug logging (BRIN_elog) provides visibility into range completion during builds
- The tupleIsAlive parameter is accepted but not currently used in the logic

## Simplified Source

```c
static void brinbuildCallback(Relation index, ItemPointer tid,
                             Datum *values, bool *isnull,
                             bool tupleIsAlive, void *brstate) {
    BrinBuildState *state = (BrinBuildState *) brstate;
    BlockNumber thisblock = ItemPointerGetBlockNumber(tid);

    // Check if we've moved to a new range that needs processing
    while (thisblock > state->bs_currRangeStart + state->bs_pagesPerRange - 1) {
        // Complete the current range and insert its index tuple
        form_and_insert_tuple(state);

        // Advance to next range
        state->bs_currRangeStart += state->bs_pagesPerRange;

        // Initialize summary state for new range
        brin_memtuple_initialize(state->bs_dtuple, state->bs_bdesc);
    }

    // Add current tuple's values to the range summary
    add_values_to_range(index, state->bs_bdesc, state->bs_dtuple, values, isnull);
}
```