# transformInsertStmt

## Location
src/backend/parser/analyze.c: 580 - 1007

## Overview
Transforms an INSERT statement from parse tree representation into a query tree structure that can be executed by the planner and executor.

## Definition
```c
static Query *
transformInsertStmt(ParseState *pstate, InsertStmt *stmt)
```

## Detailed Description
This function is the main entry point for transforming INSERT statements during the parse analysis phase. It handles multiple INSERT variants:

1. **INSERT ... DEFAULT VALUES** - Creates empty target list where all columns receive default values
2. **INSERT ... SELECT** - Transforms the SELECT subquery and builds target list from its output
3. **INSERT ... VALUES** (single row) - Directly transforms the VALUES list as the target list
4. **INSERT ... VALUES** (multiple rows) - Creates a VALUES RTE and references it with Vars

The function performs comprehensive processing including:
- WITH clause handling for CTEs
- Target table validation and permission checking
- Column list validation and default column generation
- Expression transformation and type coercion
- ON CONFLICT clause processing
- RETURNING clause processing

Key design considerations:
- Handles both simple VALUES and complex SELECT scenarios efficiently
- Maintains proper namespace isolation between main query and subqueries
- Ensures proper permission tracking for inserted columns
- Handles indirection (array/field assignments) correctly

## Parameters / Member Variables
- `pstate`: Parse state containing context information, namespace, and range table
- `stmt`: The parsed InsertStmt structure containing all INSERT clause information

## Dependencies
- Functions called/Symbols referenced:
  - transformWithClause (for WITH/CTE processing)  
  - setTargetTable (for target table setup)
  - checkInsertTargets (for column validation)
  - transformStmt (for SELECT subquery processing)
  - transformInsertRow (for row expression processing)
  - transformOnConflictClause (for UPSERT handling)
  - transformReturningList (for RETURNING clause)
  - addRangeTableEntryForSubquery/addRangeTableEntryForValues (RTE creation)
  - assign_query_collations (collation assignment)

- Called from (representative examples):
  - transformStmt (main statement transformation dispatcher)

## Notes and Other Information
- Sets pstate->p_is_insert = true to influence subsequent processing
- Handles the complex interaction between INSERT target columns and VALUES/SELECT sources
- Special handling for unknown-type constants to allow proper type coercion
- Supports both traditional INSERT and modern UPSERT (ON CONFLICT) functionality
- Manages range table entries carefully to support nested queries and CTEs
- Critical function in PostgreSQLs query transformation pipeline