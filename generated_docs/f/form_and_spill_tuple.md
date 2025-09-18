# form_and_spill_tuple

## Location
src/backend/access/brin/brin.c: 1997 - 2021

## Overview
Converts a deformed tuple in the build state into the on-disk format and writes it to a shared tuplesort during parallel BRIN index construction, allowing the leader process to insert it later.

## Definition
static void form_and_spill_tuple(BrinBuildState *state)

## Detailed Description
This function is the parallel counterpart to form_and_insert_tuple, specifically designed for parallel BRIN index construction. Instead of directly inserting tuples into the index, it writes them to a tuplesort structure where they can be collected and processed by the leader process later. This approach enables efficient parallel processing of BRIN index construction by allowing worker processes to prepare tuples independently.

Key behavioral differences from form_and_insert_tuple:
1. Skips empty ranges (bt_empty_range) to avoid unnecessary work in parallel builds
2. Uses tuplesort_putbrintuple() to write to a shared tuplesort instead of direct insertion
3. Allows the leader process to handle the final insertion phase after all workers complete

The function maintains the same tuple formation process using brin_form_tuple() but diverges in the handling of the resulting tuple, making it suitable for distributed processing scenarios.

## Parameters / Member Variables
- : A BrinBuildState structure containing context for parallel BRIN index construction, including:
  - bs_bdesc: BRIN descriptor with index metadata
  - bs_currRangeStart: Block number of the current range being processed
  - bs_dtuple: In-memory deformed tuple containing summary data
  - bs_sortstate: Tuplesort state for parallel processing
  - bs_numtuples: Counter for processed tuples

## Dependencies
- Functions called/Symbols referenced:
  - [brin_form_tuple](../b/brin_form_tuple.md): Converts in-memory BRIN summary data into serialized format
  - [tuplesort_putbrintuple](../t/tuplesort_putbrintuple.md): Writes BRIN tuple to tuplesort for later processing
  - [pfree](../p/pfree.md): Memory deallocation function
  - [BrinBuildState](../B/BrinBuildState.md): Build state structure type
  - [BrinTuple](../B/BrinTuple.md): On-disk tuple structure type

- Called from (representative examples):
  - [brinbuildCallbackParallel](../b/brinbuildCallbackParallel.md): Parallel callback function during BRIN index build
  - [_brin_parallel_scan_and_build](../b/_brin_parallel_scan_and_build.md): Main parallel BRIN index construction function

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- Specifically designed for parallel BRIN index construction workflows
- Includes an optimization to skip empty ranges (bt_empty_range check) which is not present in the serial version
- The tuplesort mechanism allows for efficient batching and sorting of tuples before final insertion
- Memory allocated by brin_form_tuple() is properly freed using pfree() to prevent memory leaks
- The leader process will later read from the tuplesort and perform the actual index insertions
- Part of PostgreSQL's parallel index building infrastructure for BRIN indexes