# mergeruns

## Location
[src/backend/utils/sort/tuplesort.c:2045-2231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2045-L2231)

## Overview
Implements the balanced k-way merge algorithm to merge all completed initial runs into a final sorted result, handling both single-pass and multi-pass external sorting scenarios.

## Definition

```c
enum;
```
## Detailed Description
The  function is the core implementation of PostgreSQL's external merge sort algorithm. It takes multiple sorted runs that have been written to tape and merges them into progressively fewer, longer runs until a single sorted result remains.

**Key Operations:**
1. **Abbreviation Management**: Disables abbreviated key comparisons since abbreviated keys aren't stored on disk
2. **Memory Reorganization**: Frees the large memtuples array and resets tuple memory context to prepare for merge operations
3. **Slab Allocator Setup**: Initializes efficient fixed-size allocation for tuple headers during merge
4. **Buffer Management**: Redistributes available memory among input and output tape buffers for optimal I/O performance
5. **Multi-Pass Merge**: Executes merge passes until only one run remains, converting outputs from one pass into inputs for the next

**Optimization Features:**
- **Final Merge Optimization**: If conditions allow (no random access needed, single run per tape), performs the final merge on-the-fly without writing to tape
- **Memory Distribution**: Dynamically allocates tape buffer memory based on the number of input/output tapes
- **Worker Support**: Handles both standalone and parallel worker contexts

The algorithm continues until all input runs are consumed and only one output run exists, representing the complete sorted dataset.

## Parameters / Member Variables
- : Pointer to the  structure containing:
  - : Must be  when called
  - : Should be 0 (all tuples written to tape)
  - /: Number of input/output tapes
  - /: Number of runs on input/output tapes
  - : Collection of logical tapes
  - : Available memory for tape buffers
  - : Final tape containing sorted result

## Dependencies
- Functions called/Symbols referenced:
  - : Sets up efficient tuple allocation
  - : Chooses next output tape for runs
  - : Merges one run from each input tape
  - : Initializes final merge state
  - /: Tape management
  - : Finalizes result tape
  - : Calculates optimal buffer sizes
  - /: Memory management
  - /: Memory usage tracking

- Called from (representative examples):
  - : Main sorting entry point
  - : When initiating external sort

## Notes and Other Information
- This is a static function within tuplesort.c, internal to the sorting implementation
- Implements the balanced k-way merge which is optimal for external sorting
- Handles both single-pass and multi-pass merging depending on available memory and run count
- Critical performance optimizations include final merge on-the-fly and dynamic buffer allocation
- Supports both standalone and parallel worker execution contexts
- The function changes the sort state from  to either  or 
- Memory management is sophisticated, transitioning from tuple-based to slab-based allocation
- Part of PostgreSQL's highly optimized external sorting system for handling large datasets