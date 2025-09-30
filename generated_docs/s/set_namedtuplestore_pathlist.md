# set_namedtuplestore_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:2939-2965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2939-L2965)

## Overview
Builds the single access path for a named tuplestore RTE (Range Table Entry), handling pathlist generation for named tuplestore scans in PostgreSQL's query planner.

## Definition
```c
static void set_namedtuplestore_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for named tuplestore RTEs in PostgreSQL's query optimizer. Named tuplestores are in-memory storage structures that hold tuples and can be accessed by name, typically used in stored procedures and functions for temporary data storage that persists across function calls. This function generates the access path for scanning such tuplestores when they are referenced in queries.

The function is relatively straightforward compared to other pathlist functions because tuplestores have simple access patterns. It sets size estimates for the tuplestore and creates a single scan path, handling only the basic case of parameterization due to LATERAL references.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned
- `rel`: RelOptInfo structure representing the relation (named tuplestore) for which paths are being generated
- `rte`: RangeTblEntry representing the named tuplestore in the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - [set_namedtuplestore_size_estimates](set_namedtuplestore_size_estimates.md)
  - [add_path](../a/add_path.md)
  - [create_namedtuplestorescan_path](../c/create_namedtuplestorescan_path.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Does not support join-qual-parameterized paths for tuplestores, eliminating the need for a separate set_namedtuplestore_size phase
- Tuplestore scans do not support pushing join clauses into their quals, but can have required parameterization due to LATERAL references in their target lists
- Named tuplestores are primarily used in PL/pgSQL and other procedural languages for temporary storage
- The function calls set_namedtuplestore_size_estimates to determine the estimated number of rows and other statistics
- Creates a straightforward scan path using create_namedtuplestorescan_path
- Located in src/backend/optimizer/path/allpaths.c:2939-2965

## Simplified Source

```c
static void
set_namedtuplestore_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    Relids required_outer;

    // Set size estimates for the named tuplestore
    set_namedtuplestore_size_estimates(root, rel);

    // Handle LATERAL references as required parameterization
    // (Join clauses cannot be pushed into tuplestore scans)
    required_outer = rel->lateral_relids;

    // Create and add the tuplestore scan path
    add_path(rel, create_namedtuplestorescan_path(root, rel, required_outer));
}
```