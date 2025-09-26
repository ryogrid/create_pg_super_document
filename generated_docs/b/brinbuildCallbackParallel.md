# brinbuildCallbackParallel

## Location
[src/backend/access/brin/brin.c:1036-1094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1036-L1094)

## Overview
Per-heap-tuple callback function for parallel BRIN index builds that processes tuples and writes summary data to shared tuplesort instead of directly inserting into the index.

## Definition

```c
static void
brinbuildCallbackParallel(Relation index,
						  ItemPointer tid,
						  Datum *values,
						  bool *isnull,
						  bool tupleIsAlive,
						  void *brstate)
```
## Detailed Description
The brinbuildCallbackParallel function is a specialized version of brinbuildCallback designed for parallel index builds. Key differences from the serial version include:

1. **Shared Tuplesort Output**: Instead of directly inserting BRIN tuples into the index, completed tuples are written to a shared tuplesort structure for later processing by the leader
2. **No Empty Range Generation**: Empty ranges are not created by workers; the leader handles filling in empty ranges during the merge phase
3. **Wraparound Handling**: Supports synchronized sequential scans that may wrap around to the beginning of the relation, requiring checks for both future and past ranges
4. **Range Boundary Logic**: Uses different logic to determine range boundaries since parallel scans may process blocks in non-sequential order

The function handles range transitions by:
- Detecting when the current tuple belongs to a different range (either future or past)
- Completing and spilling the current range's summary to tuplesort
- Calculating the appropriate range start for the new block
- Re-initializing summary state for the new range

## Parameters / Member Variables
- : The BRIN index relation being built
- : ItemPointer (TID) of the current heap tuple being processed
- : Array of Datum values for the indexed attributes of the current tuple
- : Array indicating which values are NULL
- : Boolean indicating if the tuple is visible (not used in current implementation)
- : BrinBuildState structure containing parallel build state information

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md): Extracts block number from tuple ID
  - [form_and_spill_tuple](../f/form_and_spill_tuple.md): Creates and writes BRIN tuple to shared tuplesort
  - [brin_memtuple_initialize](brin_memtuple_initialize.md): Reinitializes summary tuple for new range
  - [add_values_to_range](../a/add_values_to_range.md): Accumulates tuple values into range summary
  - BRIN_elog: Debug logging for range completion
- Called from (representative examples):
  - [_brin_parallel_scan_and_build](_brin_parallel_scan_and_build.md): Parallel BRIN build coordinator function

## Notes and Other Information
- This is a static function used only within the BRIN parallel build implementation
- Handles the complexity of parallel sequential scans that may process blocks out of order
- Does not generate placeholder tuples for empty ranges - left to the leader process
- Uses shared tuplesort for coordination between parallel workers and leader
- [Range](../R/Range.md) boundary detection accounts for potential block wraparound in parallel scans
- The leader process is responsible for merging worker results and filling empty ranges
- Debug logging helps track range completion during parallel builds