# cost_recursive_union

## Location
[src/backend/optimizer/path/costsize.c:1813-1883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1813-L1883)

## Overview
Determines and returns the cost and estimated output size of performing a recursive union operation, which is used in recursive Common Table Expressions (CTEs).

## Definition

```c
union(Path *runion, Path *nrterm, Path *rterm)
{
	Cost		startup_cost;
	Cost		total_cost;
	double		total_rows;

	/* We probably have decent estimates for the non-recursive term */
	startup_cost = nrterm->startup_cost;
	total_cost = nrterm->total_cost;
	total_rows = nrterm->rows;

	/*
	 * We arbitrarily assume that about 10 recursive iterations will be
	 * needed, and that we've managed to get a good fix on the cost and output
	 * size of each one of them.  These are mighty shaky assumptions but it's
	 * hard to see how to do better.
	 */
	total_cost += 10 * rterm->total_cost;
	total_rows += 10 * rterm->rows;

	/*
	 * Also charge cpu_tuple_cost per row to account for the costs of
	 * manipulating the tuplestores.  (We don't worry about possible
	 * spill-to-disk costs.)
	 */
	total_cost += cpu_tuple_cost * total_rows;

	runion->startup_cost = startup_cost;
	runion->total_cost = total_cost;
	runion->rows = total_rows;
	runion->pathtarget->width = Max(nrterm->pathtarget->width,
									rterm->pathtarget->width);
}

/*
 * cost_tuplesort
 *	  Determines and returns the cost of sorting a relation using tuplesort,
 *    not including the cost of reading the input data.
 *
 * If the total volume of data to sort is less than sort_mem, we will do
 * an in-memory sort, which requires no I/O and about t*log2(t) tuple
 * comparisons for t tuples.
 *
 * If the total volume exceeds sort_mem, we switch to a tape-style merge
 * algorithm.  There will still be about t*log2(t) tuple comparisons in
 * total, but we will also need to write and read each tuple once per
 * merge pass.  We expect about ceil(logM(r)) merge passes where r is the
 * number of initial runs formed and M is the merge order used by tuplesort.c.
 * Since the average initial run should be about sort_mem, we have
 *		disk traffic = 2 * relsize * ceil(logM(p / sort_mem))
 *		cpu = comparison_cost * t * log2(t)
 *
 * If the sort is bounded (i.e., only the first k result tuples are needed)
 * and k tuples can fit into sort_mem, we use a heap method that keeps only
 * k tuples in the heap;
```
## Detailed Description
This function calculates the execution cost for a recursive union operation, which is the core mechanism behind recursive CTEs in PostgreSQL. The cost estimation involves:

1. **Non-recursive term**: Uses actual cost estimates from the non-recursive (anchor) query
2. **Recursive iterations**: Makes an assumption of approximately 10 recursive iterations, multiplying the recursive term costs accordingly
3. **Tuplestore manipulation**: Adds costs for managing tuplestores that hold intermediate results between iterations
4. **Output estimation**: Combines row estimates from both terms with the iteration multiplier

The function acknowledges that the assumptions are "mighty shaky" but represents the best approximation possible given the inherent unpredictability of recursive query behavior.

## Parameters / Member Variables
- `*runion`: The Path node for the recursive union to store calculated costs and row estimates
- `*nrterm`: Path for the non-recursive (anchor) term of the recursive CTE
- `*rterm`: Path for the recursive term that will be executed iteratively
## Dependencies
- Functions called/Symbols referenced:
  - cpu_tuple_cost (global cost parameter for tuple processing)
  - Max (macro for maximum value comparison)
- Types referenced:
  - Cost (cost calculation type)
  - [Path](../P/Path.md) (query path structure)
- Called from:
  - [create_recursiveunion_path](create_recursiveunion_path.md) (in pathnode.c:3647)

## Notes and Other Information
- Uses a hardcoded assumption of 10 recursive iterations, which the code acknowledges as a rough approximation
- The startup cost equals the non-recursive term's startup cost since that must complete before recursion begins
- Includes tuplestore manipulation costs via cpu_tuple_cost for all produced rows
- Does not account for potential spill-to-disk costs of tuplestores, assuming in-memory operation
- Sets the path width to the maximum width between non-recursive and recursive terms
- The cost model is inherently imprecise due to the unpredictable nature of recursive query convergence
- Total cost formula: nrterm_cost + (10 × rterm_cost) + (cpu_tuple_cost × total_rows)
- Represents one of the more challenging areas of PostgreSQL cost estimation due to runtime variability

## Simplified Source

```c
void
cost_recursive_union(Path *runion, Path *nrterm, Path *rterm)
{
    Cost startup_cost;
    Cost total_cost;
    double total_rows;

    // Start with non-recursive term costs (these are reliable)
    startup_cost = nrterm->startup_cost;
    total_cost = nrterm->total_cost;
    total_rows = nrterm->rows;

    // Assume 10 recursive iterations (rough approximation)
    total_cost += 10 * rterm->total_cost;
    total_rows += 10 * rterm->rows;

    // Add tuplestore manipulation costs
    total_cost += cpu_tuple_cost * total_rows;

    // Set final costs and estimates
    runion->startup_cost = startup_cost;
    runion->total_cost = total_cost;
    runion->rows = total_rows;
    runion->pathtarget->width = Max(nrterm->pathtarget->width,
                                   rterm->pathtarget->width);
}
```