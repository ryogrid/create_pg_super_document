# set_tablefunc_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:2836-2859](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2836-L2859)

## Overview
Builds the single access path for a table func RTE (Range Table Entry), handling pathlist generation for table function scans in PostgreSQL's query planner.

## Definition
```c
static void set_tablefunc_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for table function RTEs in PostgreSQL's query optimizer. Table functions are special SQL constructs like XMLTABLE, JSON_TABLE, or other functions that return table-like results. The function handles the case where such constructs are used as data sources in queries.

Similar to other specialized scan types, table function scans have limited optimization opportunities. They cannot have join clauses pushed down into their quals, but they can still require parameterization due to LATERAL references in their expressions, which allows them to reference columns from outer query levels.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned
- `rel`: RelOptInfo structure representing the relation (table function) for which paths are being generated
- `rte`: RangeTblEntry representing the table function in the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - [add_path](../a/add_path.md)
  - [create_tablefuncscan_path](../c/create_tablefuncscan_path.md)
- Called from (representative examples):
  - [set_rel_pathlist](set_rel_pathlist.md)

## Notes and Other Information
- Table function scans do not support pushing join clauses into their quals, limiting optimization opportunities
- Required parameterization can occur due to LATERAL references in the function expression
- Table functions include constructs like XMLTABLE, JSON_TABLE, and other SQL standard table functions
- The function generates a single access path using create_tablefuncscan_path
- Located in src/backend/optimizer/path/allpaths.c:2836-2859

## Simplified Source

```c
static void set_tablefunc_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte) {
    Relids required_outer;

    // Table functions only support LATERAL parameterization
    required_outer = rel->lateral_relids;

    // Create single table function scan path
    add_path(rel, create_tablefuncscan_path(root, rel, required_outer));
}
```