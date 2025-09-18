# repairDependencyLoop

## Location
src/bin/pg_dump/pg_dump_sort.c: 1149 - 1473

## Overview
The repairDependencyLoop function resolves circular dependencies in PostgreSQL dump objects by identifying specific dependency patterns and applying appropriate repair strategies, or breaking the loop arbitrarily if no principled solution exists.

## Definition


## Detailed Description
This function is the central dependency loop resolution mechanism in pg_dump's sorting system. It handles various types of circular dependencies that can occur between database objects during dump ordering. The function uses a pattern-matching approach to identify common dependency loop scenarios and delegates to specialized repair functions for each case.

The function processes loops in order of specificity, starting with well-understood 2-object loops and progressing to more complex multi-object scenarios. For each pattern, it attempts to find a safe way to break the dependency without compromising the logical integrity of the database dump.

When no recognized pattern is found, the function logs a warning and breaks the loop arbitrarily, which may require manual intervention during restore (such as using --disable-triggers).

## Parameters / Member Variables
- : Array of pointers to DumpableObject structures representing the objects in the dependency loop
- : Number of objects in the dependency loop

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