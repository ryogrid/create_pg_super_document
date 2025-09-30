# make_valuesscan

## Location
[src/backend/optimizer/plan/createplan.c:5744-5762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5744-L5762)

## Overview
Creates and initializes a ValuesScan plan node, which represents a scan operation on a VALUES clause that provides literal row data directly in the SQL query.

## Definition
```c
static ValuesScan *
make_valuesscan(List *qptlist,
                List *qpqual,
                Index scanrelid,
                List *values_lists)
```

## Detailed Description
The `make_valuesscan` function is a factory function that constructs a ValuesScan plan node. This node type is used when the query planner needs to scan data provided directly in a VALUES clause, such as `VALUES (1, 'a'), (2, 'b'), (3, 'c')`. The function allocates memory for a new ValuesScan node, initializes its base Plan structure with the provided target list and qualification conditions, and stores the list of value expressions that represent the rows of data to be scanned.

This scan type is commonly used for queries that provide literal data inline, table value constructors, or when VALUES appears in the FROM clause of a query.

## Parameters / Member Variables
- `qptlist`: The target list (projection list) specifying which columns/expressions to return from the values scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to be applied during the scan
- `scanrelid`: The relation ID assigned to this scan operation for identification purposes
- `values_lists`: A list of lists, where each inner list represents the expressions for one row of the VALUES clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate ValuesScan node)
  - [ValuesScan](../V/ValuesScan.md) (node type)
- Called from (representative examples):
  - [create_valuesscan_plan](../c/create_valuesscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's an internal helper for plan creation
- The function follows PostgreSQL's pattern of setting lefttree and righttree to NULL for leaf scan nodes
- The values_lists parameter contains the actual data expressions that will be evaluated at execution time
- VALUES scans are often used in INSERT statements, but can also appear in SELECT queries for generating test data or small lookup tables
- Part of PostgreSQL's query planner infrastructure that handles literal data specified directly in SQL queries

## Simplified Source

```c
// Simplified version of make_valuesscan
static ValuesScan *
make_valuesscan(List *target_list, List *qualifiers,
                Index scan_relation_id, List *values_lists) {
    // Create new ValuesScan node
    ValuesScan *values_scan = makeNode(ValuesScan);
    Plan *plan = &values_scan->scan.plan;

    // Initialize basic plan structure
    plan->targetlist = target_list;
    plan->qual = qualifiers;
    plan->lefttree = NULL;  // Leaf node
    plan->righttree = NULL; // Leaf node

    // Set VALUES-specific properties
    values_scan->scan.scanrelid = scan_relation_id;
    values_scan->values_lists = values_lists;

    return values_scan;
}
```

Key simplifications made:
- Used more descriptive parameter names for clarity
- Added comments explaining the core logic steps
- Maintained the essential structure initialization pattern
- Preserved all functional behavior while improving readability