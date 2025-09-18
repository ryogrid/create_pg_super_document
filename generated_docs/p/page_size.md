# page_size

## Location
src/backend/optimizer/path/costsize.c: 6356 - 6365

## Overview
Estimates the number of pages required to store a given number of tuples with a specified width.

## Definition
```c
static double page_size(double tuples, int width)
```

## Detailed Description
This function calculates how many database pages are needed to store a specified number of tuples. It uses the relation_byte_size function to determine the total storage space required in bytes, then divides by BLCKSZ (the block size) and applies the ceiling function to round up to the nearest whole page. This ensures that partial pages are counted as full pages, which is necessary for accurate storage estimation.

## Parameters / Member Variables
- `tuples`: The estimated number of tuples to be stored (as a double for fractional estimates)
- `width`: The average width in bytes of each tuple's data

## Dependencies
- Functions called/Symbols referenced:
  - relation_byte_size (calculates total byte storage requirement)
  - BLCKSZ (constant defining the database block/page size)
  - ceil (math function to round up to nearest integer)
- Called from (representative examples):
  - cost_qual_eval_context
  - initial_cost_hashjoin

## Notes and Other Information
- This is a static function used internally within the cost estimation module
- The function always rounds up using ceil() since partial pages still require a full page allocation
- Critical for memory and I/O cost estimation in query planning
- Works in conjunction with relation_byte_size to provide page-level storage estimates
- Used primarily in hash join costing where page-level estimates are important for memory allocation decisions