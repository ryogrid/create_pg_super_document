# TidScan

## Location
src/include/nodes/plannodes.h: 552 - 556

## Overview
TidScan is a PostgreSQL plan node that directly accesses specific tuples using their CTID (tuple identifier) values, providing the fastest possible access to known tuple locations.

## Definition
```c
typedef struct TidScan
{
    Scan        scan;
    List       *tidquals;       /* qual(s) involving CTID = something */
} TidScan;
```

## Detailed Description
TidScan provides direct tuple access by physical location using CTID (tuple identifier) values. This is the most efficient way to retrieve specific tuples when their physical locations are known. TidScan is typically used in scenarios where the query contains explicit CTID comparisons or cursor-based operations that reference specific tuple positions.

The tidquals field contains an implicitly OR'ed list of qualification expressions that specify which tuples to retrieve. These can take several forms: direct CTID equality comparisons with pseudoconstants, CTID comparisons with arrays using ANY clauses, or CurrentOfExpr nodes for cursor operations.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scan node fields including relation information and target list
- `tidquals`: List of qualification expressions involving CTID comparisons, such as "CTID = pseudoconstant", "CTID = ANY(pseudoconstant_array)", or CurrentOfExpr for positioned cursor operations

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
- Called from (representative examples):
  - ExplainNode (for EXPLAIN output)
  - ExecInitNode (executor initialization)
  - TidExprListCreate (expression processing)
  - ExecInitTidScan (node initialization)
  - create_tidscan_plan (plan creation)
  - make_tidscan (plan node construction)

## Notes and Other Information
- TidScan provides the fastest possible tuple access since it bypasses all indexing mechanisms and goes directly to the physical tuple location
- Commonly used with CURRENT OF cursor operations where the application needs to update or delete the tuple at the cursor's current position
- The CTID values must be compile-time constants or parameters; they cannot be computed from other table columns during execution
- TidScan operations are inherently limited to single relations and cannot be used in join operations
- Multiple CTID values in tidquals are processed with OR semantics, allowing retrieval of multiple specific tuples in a single scan
- This scan type is particularly useful for debugging queries, administrative operations, and applications that maintain their own tuple location tracking