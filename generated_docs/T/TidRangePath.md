# TidRangePath

## Location
src/include/nodes/pathnodes.h: 1835 - 1839

## Overview
TidRangePath represents an access path for scanning a table using a contiguous range of tuple identifiers (TIDs), enabling efficient retrieval of rows based on CTID range conditions.

## Definition

```c
typedef struct TidRangePath
{
	Path		path;
	List	   *tidrangequals;
} TidRangePath;
```
## Detailed Description
TidRangePath is a specialized scan path node used in PostgreSQL's query planner to represent table access via TID (Tuple Identifier) range scanning. This path type is utilized when queries contain WHERE clauses that specify ranges of CTIDs using comparison operators (>, >=, <, <=). The scan operates by reading a contiguous range of tuple identifiers, which can be more efficient than full table scans when the TID range is selective.

The TID range scan is particularly useful for queries that need to access specific physical locations within a table based on CTID predicates. Unlike regular index scans, TID range scans directly target physical tuple locations, making them suitable for maintenance operations or queries with explicit CTID constraints.

## Parameters / Member Variables
- : Base Path structure containing common path information (cost estimates, parent relation, etc.)
- : List of qualifier expressions that define the TID range conditions, containing implicitly AND'ed expressions of the form "CTID relop pseudoconstant" where relop is one of >, >=, <, <=

## Dependencies
- Functions called/Symbols referenced:
  - Path (base structure)
  - List (for tidrangequals storage)
- Called from (representative examples):
  - create_tidrangescan_path (creates TidRangePath instances)
  - create_tidrangescan_plan (converts TidRangePath to execution plan)
  - create_scan_plan (general scan plan creation)

## Notes and Other Information
- Always produces unordered results (pathkeys = NIL)
- Not parallel-aware by default
- Requires base relations only (not applicable to joins)
- TID range qualifiers have AND semantics
- Used primarily for maintenance operations and explicit CTID-based queries
- More efficient than sequential scans when TID ranges are highly selective