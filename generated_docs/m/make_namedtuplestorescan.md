# make_namedtuplestorescan

## Location
[src/backend/optimizer/plan/createplan.c:5784-5803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5784-L5803)

## Overview
Creates and initializes a NamedTuplestoreScan plan node for scanning Ephemeral Named Relations (ENRs) in PostgreSQL query execution.

## Definition

```c
static NamedTuplestoreScan *
make_namedtuplestorescan(List *qptlist,
						 List *qpqual,
						 Index scanrelid,
						 char *enrname)
```
## Detailed Description
This function constructs a NamedTuplestoreScan plan node, which is used to scan temporary named tuple stores (Ephemeral Named Relations). ENRs are typically used for WITH clause subqueries, transition tables in triggers, and other temporary data structures that need to be accessed like regular tables during query execution. The function allocates a new NamedTuplestoreScan node using makeNode() and initializes its fields with the provided parameters.

## Parameters / Member Variables
- `*qptlist`: Target list specifying which columns/expressions to return from the scan
- `*qpqual`: List of qualification conditions (WHERE clause predicates) to apply during scanning
- `scanrelid`: Index identifying the relation being scanned in the query's range table
- `*enrname`: Name of the Ephemeral Named Relation to scan
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate NamedTuplestoreScan node)
  - [NamedTuplestoreScan](../N/NamedTuplestoreScan.md) (struct type)
- Called from (representative examples):
  - [create_namedtuplestorescan_plan](../c/create_namedtuplestorescan_plan.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the createplan.c file
- Cost calculation is expected to be performed by the caller before using this node
- The function sets both lefttree and righttree to NULL as this is a leaf node in the plan tree
- ENRs provide a mechanism for PostgreSQL to handle temporary, named data sets efficiently during query execution

## Simplified Source

```c
static NamedTuplestoreScan *make_namedtuplestorescan(List *qptlist, List *qpqual,
                                                    Index scanrelid, char *enrname) {
    // Create new NamedTuplestoreScan node
    NamedTuplestoreScan *node = makeNode(NamedTuplestoreScan);
    Plan *plan = &node->scan.plan;

    // Set basic plan properties
    plan->targetlist = qptlist;  // Output columns
    plan->qual = qpqual;         // Filter conditions
    plan->lefttree = NULL;       // No child plans (leaf node)
    plan->righttree = NULL;

    // Configure ENR scan specifics
    node->scan.scanrelid = scanrelid;  // Relation ID
    node->enrname = enrname;           // Named tuplestore name

    return node;
}
```