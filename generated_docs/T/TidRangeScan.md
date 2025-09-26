# TidRangeScan

## Location
[src/include/nodes/plannodes.h:565-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L565-L569)

## Overview
TidRangeScan is a PostgreSQL plan node that scans tuples within a specified range of CTID (tuple identifier) values, providing efficient access to contiguous blocks of tuples.

## Definition
```c
typedef struct TidRangeScan
{
    Scan        scan;
    List       *tidrangequals;  /* qual(s) involving CTID op something */
} TidRangeScan;
```

## Detailed Description
TidRangeScan extends the concept of TidScan by allowing range-based access to tuples using CTID comparisons. Instead of accessing specific individual tuples, TidRangeScan can efficiently scan all tuples within a specified range of physical locations. This is particularly useful for operations that need to process contiguous blocks of data or when queries specify CTID range conditions.

The tidrangequals field contains an implicitly AND'ed list of qualification expressions that define the range boundaries. These expressions use relational operators (>, >=, <, <=) with CTID and pseudoconstants to establish the upper and lower bounds of the scan range.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scan node fields including relation information and target list
- `tidrangequals`: List of qualification expressions involving CTID range operators such as "CTID > pseudoconstant" or "CTID <= pseudoconstant", combined with AND semantics to define scan boundaries

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
- Called from (representative examples):
  - ExplainNode (for EXPLAIN output)
  - ExecInitNode (executor initialization)
  - TidExprListCreate (expression processing for range conditions)
  - ExecInitTidRangeScan (node-specific initialization)
  - create_tidrangescan_plan (plan creation)
  - make_tidrangescan (plan node construction)

## Notes and Other Information
- TidRangeScan provides more flexible tuple access than TidScan by supporting range operations rather than just equality
- The scan processes tuples in CTID order, making it efficient for accessing contiguous blocks of data
- Range conditions are combined with AND semantics, allowing specification of both upper and lower bounds
- Particularly useful for administrative operations, bulk data processing, or debugging scenarios where specific physical regions of a table need examination
- Like TidScan, the CTID boundary values must be compile-time constants or parameters
- The scan can handle open-ended ranges (e.g., only upper bound or only lower bound specified)
- Supports the same relational operators as regular CTID comparisons: >, >=, <, and <=
- More efficient than multiple individual TidScan operations when dealing with ranges of consecutive tuples