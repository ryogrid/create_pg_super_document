# repairDependencyLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1149-1473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1149-L1473)

## Overview
The repairDependencyLoop function resolves circular dependencies in PostgreSQL dump objects by identifying specific dependency patterns and applying appropriate repair strategies, or breaking the loop arbitrarily if no principled solution exists.

## Definition

```c
static void
repairDependencyLoop(DumpableObject **loop,
					 int nLoop)
```
## Detailed Description
This function is the central dependency loop resolution mechanism in pg_dump's sorting system. It handles various types of circular dependencies that can occur between database objects during dump ordering. The function uses a pattern-matching approach to identify common dependency loop scenarios and delegates to specialized repair functions for each case.

The function processes loops in order of specificity, starting with well-understood 2-object loops and progressing to more complex multi-object scenarios. For each pattern, it attempts to find a safe way to break the dependency without compromising the logical integrity of the database dump.

When no recognized pattern is found, the function logs a warning and breaks the loop arbitrarily, which may require manual intervention during restore (such as using --disable-triggers).

## Parameters / Member Variables
- `**loop`: Array of pointers to DumpableObject structures representing the objects in the dependency loop
- `nLoop`: Number of objects in the dependency loop
## Dependencies
- Functions called/Symbols referenced:
  - [repairTypeFuncLoop](repairTypeFuncLoop.md)
  - [repairViewRuleLoop](repairViewRuleLoop.md)
  - [repairViewRuleMultiLoop](repairViewRuleMultiLoop.md)
  - [repairMatViewBoundaryMultiLoop](repairMatViewBoundaryMultiLoop.md)
  - [repairFunctionBoundaryMultiLoop](repairFunctionBoundaryMultiLoop.md)
  - [repairTableConstraintLoop](repairTableConstraintLoop.md)
  - [repairTableConstraintMultiLoop](repairTableConstraintMultiLoop.md)
  - [repairTableAttrDefLoop](repairTableAttrDefLoop.md)
  - [repairIndexLoop](repairIndexLoop.md)
  - [repairTableAttrDefMultiLoop](repairTableAttrDefMultiLoop.md)
  - [repairDomainConstraintLoop](repairDomainConstraintLoop.md)
  - [repairDomainConstraintMultiLoop](repairDomainConstraintMultiLoop.md)
  - [removeObjectDependency](removeObjectDependency.md)
  - [describeDumpableObject](../d/describeDumpableObject.md)
  - pg_log_warning
  - pg_log_warning_detail
  - pg_log_warning_hint
  - ngettext
- Called from (representative examples):
  - [findDependencyLoops](../f/findDependencyLoops.md)

## Notes and Other Information
- Handles specific dependency loop patterns including:
  - Datatype and I/O functions (2-object loops)
  - Views/materialized views and their ON SELECT rules
  - Tables and CHECK constraints
  - Tables and attribute defaults
  - Index inheritance relationships
  - Domain and domain constraints
  - Circular foreign key constraints
- For foreign key constraint loops, provides specific guidance about using --disable-triggers
- Self-dependencies (nLoop == 1) are handled as special cases
- The function prioritizes maintaining data integrity over strict dependency ordering
- Located in src/bin/pg_dump/pg_dump_sort.c:1149-1473

## Simplified Source

```c
static void repairDependencyLoop(DumpableObject **loop, int nLoop) {
    // Handle 2-object loops first (most common cases)
    if (nLoop == 2) {
        // Type and I/O function dependency
        if ((loop[0]->objType == DO_TYPE && loop[1]->objType == DO_FUNC) ||
            (loop[1]->objType == DO_TYPE && loop[0]->objType == DO_FUNC)) {
            repairTypeFuncLoop(/* appropriate order */);
            return;
        }

        // View and its ON SELECT rule
        if (/* view and rule pattern match */) {
            repairViewRuleLoop(/* appropriate order */);
            return;
        }

        // Table and CHECK constraint
        if (/* table and constraint pattern match */) {
            repairTableConstraintLoop(/* appropriate order */);
            return;
        }

        // Table and attribute default
        if (/* table and attrdef pattern match */) {
            repairTableAttrDefLoop(/* appropriate order */);
            return;
        }

        // Index inheritance relationships
        if (/* index parent-child pattern match */) {
            repairIndexLoop(/* appropriate order */);
            return;
        }

        // Domain and its constraints
        if (/* domain and constraint pattern match */) {
            repairDomainConstraintLoop(/* appropriate order */);
            return;
        }
    }

    // Handle multi-object loops (nLoop > 2)
    if (nLoop > 2) {
        // Find and repair view-rule multi-loops
        if (/* view and rule found in loop */) {
            repairViewRuleMultiLoop(view, rule);
            return;
        }

        // Find and repair matview-boundary multi-loops
        if (/* matview and boundary found in loop */) {
            repairMatViewBoundaryMultiLoop(boundary, nextobj);
            return;
        }

        // Find and repair function-boundary multi-loops
        if (/* function and boundary found in loop */) {
            repairFunctionBoundaryMultiLoop(boundary, nextobj);
            return;
        }

        // Find and repair table-constraint multi-loops
        if (/* table and constraint found in loop */) {
            repairTableConstraintMultiLoop(table, constraint);
            return;
        }

        // Find and repair table-attrdef multi-loops
        if (/* table and attrdef found in loop */) {
            repairTableAttrDefMultiLoop(table, attrdef);
            return;
        }

        // Find and repair domain-constraint multi-loops
        if (/* domain and constraint found in loop */) {
            repairDomainConstraintMultiLoop(domain, constraint);
            return;
        }
    }

    // Handle self-dependencies (nLoop == 1)
    if (nLoop == 1 && loop[0]->objType == DO_TABLE) {
        removeObjectDependency(loop[0], loop[0]->dumpId);
        return;
    }

    // Handle circular foreign key constraints
    bool all_table_data = true;
    for (int i = 0; i < nLoop; i++) {
        if (loop[i]->objType != DO_TABLE_DATA) {
            all_table_data = false;
            break;
        }
    }

    if (all_table_data) {
        pg_log_warning("circular foreign-key constraints detected");
        // Log table names and provide restoration hints
        removeObjectDependency(loop[0],
                             nLoop > 1 ? loop[1]->dumpId : loop[0]->dumpId);
        return;
    }

    // No pattern matched - break loop arbitrarily
    pg_log_warning("could not resolve dependency loop - breaking arbitrarily");
    removeObjectDependency(loop[0],
                         nLoop > 1 ? loop[1]->dumpId : loop[0]->dumpId);
}
```