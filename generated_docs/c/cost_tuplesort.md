# cost_tuplesort

## Location
[src/backend/optimizer/path/costsize.c:1884-1985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1884-L1985)

## Overview
Determines and returns the cost of sorting a relation using PostgreSQL's tuplesort algorithm, excluding the cost of reading the input data.

## Definition

```c
static void
cost_tuplesort(Cost *startup_cost, Cost *run_cost,
			   double tuples, int width,
			   Cost comparison_cost, int sort_mem,
			   double limit_tuples)
```
## Detailed Description
The function calculates sorting costs using different algorithms depending on data size and available memory:

1. **In-memory sort**: When total data volume is less than sort_mem, performs quicksort requiring t*log2(t) tuple comparisons for t tuples.

2. **Disk-based sort**: When data exceeds sort_mem, uses a tape-style merge algorithm with approximately t*log2(t) comparisons plus disk I/O for writing and reading tuples across multiple merge passes.

3. **Bounded heap sort**: When only the first k result tuples are needed and k tuples fit in sort_mem, uses a heap method requiring about t*log2(k) comparisons.

The function assumes disk traffic is 3/4 sequential and 1/4 random accesses, and charges two operator evaluations per tuple comparison by default.

## Parameters / Member Variables
- : Output parameter for the startup cost of the sort operation
- : Output parameter for the per-tuple cost during sort execution
- : Number of tuples in the relation to be sorted
- : Average tuple width in bytes
- : Extra cost per comparison beyond the default
- : Number of kilobytes of work memory allocated for the sort
- : Bound on number of output tuples; -1 if no bound

## Dependencies
- Functions called/Symbols referenced:
  - [relation_byte_size](../r/relation_byte_size.md)
  - [tuplesort_merge_order](../t/tuplesort_merge_order.md)
  - LOG2
  - Cost (type)
- Called from (representative examples):
  - [cost_incremental_sort](cost_incremental_sort.md)
  - [cost_sort](cost_sort.md)

## Notes and Other Information
- Ensures sort cost is never estimated as zero by setting minimum tuple count to 2.0
- Default comparison cost includes 2.0 * cpu_operator_cost
- Uses logarithmic merge calculations: logM(r) = log(r) / log(M) where M is merge order
- Run cost charges cpu_operator_cost per tuple since Sort nodes have less overhead than most plan nodes
- Critical for query planner's decision-making in choosing between different sort strategies

## Simplified Source

```c
static void
cost_tuplesort(Cost *startup_cost, Cost *run_cost,
               double tuples, int width,
               Cost comparison_cost, int sort_mem,
               double limit_tuples)
{
    double input_bytes = relation_byte_size(tuples, width);
    double output_bytes, output_tuples;
    long sort_mem_bytes = sort_mem * 1024L;

    // Ensure minimum tuple count for cost calculations
    if (tuples < 2.0)
        tuples = 2.0;

    // Add default comparison cost
    comparison_cost += 2.0 * cpu_operator_cost;

    // Determine output size based on LIMIT
    if (limit_tuples > 0 && limit_tuples < tuples) {
        output_tuples = limit_tuples;
        output_bytes = relation_byte_size(output_tuples, width);
    } else {
        output_tuples = tuples;
        output_bytes = input_bytes;
    }

    if (output_bytes > sort_mem_bytes) {
        // Disk-based sort: calculate merge passes and I/O costs
        double npages = ceil(input_bytes / BLCKSZ);
        double nruns = input_bytes / sort_mem_bytes;
        double mergeorder = tuplesort_merge_order(sort_mem_bytes);
        double log_runs = (nruns > mergeorder) ?
            ceil(log(nruns) / log(mergeorder)) : 1.0;

        *startup_cost = comparison_cost * tuples * LOG2(tuples);
        *startup_cost += 2.0 * npages * log_runs *
            (seq_page_cost * 0.75 + random_page_cost * 0.25);
    }
    else if (tuples > 2 * output_tuples || input_bytes > sort_mem_bytes) {
        // Bounded heap sort for limited output
        *startup_cost = comparison_cost * tuples * LOG2(2.0 * output_tuples);
    }
    else {
        // In-memory quicksort
        *startup_cost = comparison_cost * tuples * LOG2(tuples);
    }

    // Per-tuple processing cost
    *run_cost = cpu_operator_cost * tuples;
}
```