# get_quals_from_indexclauses

## Location
[src/backend/utils/adt/selfuncs.c:6526-6555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6526-L6555)

## Overview
Extracts the actual indexquals (as RestrictInfos) from a list of IndexClause structures, flattening the nested structure into a simple list of restriction conditions.

## Definition
```c
List *get_quals_from_indexclauses(List *indexclauses)
```

## Detailed Description
This utility function processes a list of IndexClause structures and extracts all the individual index qualification conditions (indexquals) contained within them. Each IndexClause can contain multiple RestrictInfo nodes representing different qualification conditions that can be satisfied by the index. The function iterates through each IndexClause and then through each indexqual within that clause, collecting all RestrictInfo nodes into a single flattened list. This is commonly used in index cost estimation functions where the planner needs to work with individual restriction conditions rather than the hierarchical IndexClause structure.

## Parameters / Member Variables
- `indexclauses`: List of IndexClause structures containing index qualification conditions

## Dependencies
- Functions called/Symbols referenced:
  - [IndexClause](../I/IndexClause.md)
  - Cost
- Called from (representative examples):
  - [genericcostestimate](genericcostestimate.md)
  - [gincostestimate](gincostestimate.md)
  - [brincostestimate](../b/brincostestimate.md)

## Notes and Other Information
This function is part of PostgreSQL's index cost estimation framework and is used by various index access method cost estimation functions. The function performs a straightforward flattening operation, converting the two-level structure (IndexClauses containing lists of indexquals) into a single-level list of RestrictInfo nodes. This flattened representation is often more convenient for subsequent processing steps that need to examine individual qualification conditions. The function is non-static, making it available for use by different index access methods and cost estimation routines throughout the codebase.