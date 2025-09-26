# IndexOnlyScan

## Location
[src/include/nodes/plannodes.h:492-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L492-L501)

## Overview
IndexOnlyScan represents an index-only scan plan node that retrieves data directly from index pages without accessing the underlying heap table, providing optimal performance when all required columns are available in the index.

## Definition

```c
typedef struct IndexOnlyScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (usually OpExprs) */
	List	   *recheckqual;	/* index quals in recheckable form */
	List	   *indexorderby;	/* list of index ORDER BY exprs */
	List	   *indextlist;		/* TargetEntry list describing index's cols */
	ScanDirection indexorderdir;	/* forward or backward or don't care */
} IndexOnlyScan;
```
## Detailed Description
The IndexOnlyScan structure represents an optimized index-based scan operation that retrieves all required data directly from index pages without accessing the heap table. This scan type is only possible when the index contains all columns needed by the query (covering index). It provides significant performance benefits by reducing I/O operations and avoiding heap table access.

Unlike regular IndexScan, all variables in IndexOnlyScan reference index columns (varno = INDEX_VAR), and the scan includes specialized recheck capabilities for handling lossy index operators when some index columns are not directly retrievable.

## Parameters / Member Variables
- : The base Scan structure containing the Plan node and scanrelid
- : The OID of the specific index to scan that contains all required columns
- : List of index qualification expressions using index column variables
- : Index qualification expressions in recheckable form, using only retrievable index columns for handling lossy operators
- : List of ORDER BY expressions for ordered index scans
- : Target entry list describing the index columns, used by EXPLAIN and containing base table variable references (marked resjunk if not reconstructible)
- : Scan direction (forward, backward, or don't care) for ordered results

## Dependencies
- Functions called/Symbols referenced:
  - [Scan](../S/Scan.md) (inherited base structure)
  - ScanDirection (enumeration for scan direction)
  - Oid (object identifier type)
  - [List](../L/List.md) (PostgreSQL list type)

- Called from (representative examples):
  - [ExecInitIndexOnlyScan](../E/ExecInitIndexOnlyScan.md) (executor initialization for index-only scans)
  - [IndexOnlyNext](IndexOnlyNext.md) (main index-only scan execution function)
  - [make_indexonlyscan](../m/make_indexonlyscan.md) (planner utility to create IndexOnlyScan nodes)
  - [set_indexonlyscan_references](../s/set_indexonlyscan_references.md) (reference setting for index-only scans)
  - [ExplainNode](../E/ExplainNode.md) (query explanation functionality)
  - [set_deparse_plan](../s/set_deparse_plan.md) (plan deparsing for rule utilities)

## Notes and Other Information
- Most efficient scan type when the index contains all required query columns (covering index)
- Eliminates heap table access, significantly reducing I/O operations
- All plan node variables reference index columns, not base table columns
- The recheckqual mechanism handles cases where some index operators are lossy
- indextlist helps EXPLAIN display meaningful column names by mapping to base table columns
- Only viable when the index access method can reconstruct all needed column values
- Requires visibility map checks in some cases to ensure tuple visibility without heap access
- The actual execution logic is implemented in src/backend/executor/nodeIndexonlyscan.c
- Cannot be used if any required columns are not available in the index