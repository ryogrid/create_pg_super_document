# heapam_estimate_rel_size

## Location
src/backend/access/heap/heapam_handler.c: 2105 - 2121

## Overview
Estimates the physical size of a heap-based relation by providing statistics on pages, tuples, and visibility fraction.

## Definition


## Detailed Description
This function is part of the heap table access method interface and provides size estimation for heap-based relations. It serves as a wrapper that delegates the actual estimation work to the generic  function, providing heap-specific overhead constants. The function calculates estimates for the number of pages, tuples, and all-visible fraction based on the relation's current statistics and the provided attribute width information.

## Parameters / Member Variables
- : The relation for which to estimate size
- : Array of average widths for each attribute in the relation
- : Output parameter for estimated number of pages
- : Output parameter for estimated number of tuples
- : Output parameter for estimated fraction of all-visible pages

## Dependencies
- Functions called/Symbols referenced:
  - [table_block_relation_estimate_size](../t/table_block_relation_estimate_size.md)
  - HEAP_OVERHEAD_BYTES_PER_TUPLE (constant)
  - HEAP_USABLE_BYTES_PER_PAGE (constant)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (as part of table access method interface)

## Notes and Other Information
- This function is part of the heap table access method (tableam) interface
- It uses heap-specific constants for overhead calculations: HEAP_OVERHEAD_BYTES_PER_TUPLE and HEAP_USABLE_BYTES_PER_PAGE
- The function is static, indicating it's only used within the heapam_handler.c file as part of the table access method dispatch
- Size estimation is crucial for query planning and optimization decisions