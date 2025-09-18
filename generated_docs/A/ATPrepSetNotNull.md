# ATPrepSetNotNull

## Location
[src/backend/commands/tablecmds.c:7691-7759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7691-L7759)

## Overview
ATPrepSetNotNull is a preparation phase function for the ALTER TABLE ALTER COLUMN SET NOT NULL command, handling recursion logic and optimization for partitioned tables before the actual constraint enforcement.

## Definition


## Detailed Description
This function prepares for setting a NOT NULL constraint on a column during ALTER TABLE operations. It implements several key optimizations:

1. **Recursion Control**: If already recursing, it returns early to avoid duplicate processing since ATSimpleRecursion handles all children.

2. **Partitioned Table Optimization**: For partitioned tables with children, it checks if the target column is already NOT NULL. If so, it skips recursion entirely, avoiding unnecessary per-child locks and improving concurrency in parallel restore scenarios.

3. **Special Partitioned Table Handling**: For partitioned tables with ALTER TABLE ONLY ... SET NOT NULL, it converts the operation to CHECK NOT NULL for all children rather than using normal recursion.

4. **Traditional Inheritance Limitation**: The optimization only applies to partitioned tables since traditional inheritance doesn't enforce NOT NULL constraints consistently between parent and child tables.

## Parameters / Member Variables
- : Work queue for queueing additional ALTER TABLE subcommands
- : The relation being altered
- : The ALTER TABLE command structure containing the column name
- : Whether to apply the command to child tables
- : Flag indicating if this call is part of an ongoing recursion
- : Lock mode to use for accessing child relations
- : ALTER TABLE utility context for the operation

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (to check column existence and NOT NULL status)
  - [ATSimpleRecursion](ATSimpleRecursion.md) (for normal recursion logic)
  - makeNode (to create new AlterTableCmd)
  - [pstrdup](../p/pstrdup.md) (to duplicate column name string)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (main ALTER TABLE command preparation)
  - [ATParseTransformCmd](ATParseTransformCmd.md) (command parsing and transformation)

## Notes and Other Information
- This function is part of the two-phase ALTER TABLE processing (preparation and execution phases)
- The optimization for partitioned tables with existing NOT NULL columns can significantly improve performance in bulk operations
- The function handles a design limitation where traditional inheritance doesn't properly enforce NOT NULL constraints from parent to child tables
- For partitioned tables with ONLY clause, it transforms SET NOT NULL to CHECK NOT NULL for children to maintain proper constraint validation