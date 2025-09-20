# relation_byte_size

## Location
[src/backend/optimizer/path/costsize.c:6345-6355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L6345-L6355)

## Overview
Estimates the storage space in bytes required for a given number of tuples with a specified width.

## Definition

```c
static double
relation_byte_size(double tuples, int width)
```
## Detailed Description
This function calculates the total storage space needed for a relation by multiplying the number of tuples by the aligned size of each tuple. The calculation accounts for both the actual tuple data width and the heap tuple header overhead. The function uses PostgreSQL's alignment requirements (MAXALIGN) to ensure proper memory alignment for both the tuple data and the heap tuple header.

## Parameters / Member Variables
- : The estimated number of tuples in the relation (as a double for fractional estimates)
- : The average width in bytes of each tuple's data

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (macro for memory alignment)
  - SizeofHeapTupleHeader (constant for tuple header size)
- Called from (representative examples):
  - cost_qual_eval_context
  - [cost_tuplesort](../c/cost_tuplesort.md)
  - [cost_material](../c/cost_material.md)
  - [cost_memoize_rescan](../c/cost_memoize_rescan.md)
  - [cost_agg](../c/cost_agg.md)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)
  - [cost_rescan](../c/cost_rescan.md)
  - [page_size](../p/page_size.md)

## Notes and Other Information
- This is a static function used internally within the cost estimation module
- The function accounts for PostgreSQL's memory alignment requirements using MAXALIGN
- The calculation includes the heap tuple header overhead in addition to the actual data width
- Used extensively throughout the query optimizer for memory usage estimates in various operations like sorting, hashing, and aggregation