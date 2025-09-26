# TidPath

## Location
[src/include/nodes/pathnodes.h:1823-1827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1823-L1827)

## Overview
TidPath represents a scan by TID (tuple identifier), allowing direct access to specific tuples when their physical locations are known through CTID conditions or cursor operations.

## Definition

```c
typedef struct TidPath
{
	Path		path;
	List	   *tidquals;		/* qual(s) involving CTID = something */
} TidPath;
```
## Detailed Description
TidPath represents a specialized access path that scans tuples by their physical tuple identifiers (TIDs). This is one of the most direct and efficient ways to access specific tuples when their physical locations are known, bypassing any index structures entirely.

The path is used in scenarios where:
1. WHERE clauses contain explicit CTID comparisons (e.g., "WHERE CTID = '(0,1)'" or "WHERE CTID = ANY(ARRAY['(0,1)', '(0,2)'])")
2. Cursor operations using CurrentOfExpr for positioned updates/deletes
3. Internal operations that need to access specific tuple locations directly

TID scans are extremely fast for small numbers of known tuple locations since they avoid index overhead and go directly to the heap pages. However, they require prior knowledge of the exact physical tuple locations.

## Parameters / Member Variables
- : Base Path structure containing cost estimates, row counts, and other path properties for the TID scan
- : List of qualifier expressions that specify which TIDs to scan, containing:
  - Expressions of the form "CTID = pseudoconstant" for single TID lookups
  - Expressions of the form "CTID = ANY(pseudoconstant_array)" for multiple TID lookups
  - CurrentOfExpr nodes for cursor-based positioned operations

## Dependencies
- Functions called/Symbols referenced:
  - Path (base path structure)
  - List (PostgreSQL list structure)
  - CurrentOfExpr (for cursor operations)
  - CTID (tuple identifier type)

- Called from (representative examples):
  - create_tidscan_path (path creation)
  - create_tidscan_plan (plan creation)  
  - create_scan_plan (during scan plan selection)
  - create_bitmap_or_path (as alternative access method)

## Notes and Other Information
- TID scans are among the fastest access methods when exact tuple locations are known
- The tidquals list is implicitly OR'ed - any matching TID condition will include the tuple
- Commonly used for cursor operations (UPDATE/DELETE WHERE CURRENT OF cursor)
- Can be used with explicit CTID values in WHERE clauses for debugging or special cases
- Does not require any indexes and accesses heap pages directly
- Particularly useful for small numbers of specific tuple accesses
- TID values must be stable during the transaction (no concurrent modifications affecting physical locations)