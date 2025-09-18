# add_row_identity_columns

## Location
[src/backend/optimizer/util/appendinfo.c:884-964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L884-L964)

## Overview
Adds the standard row identity columns needed by PostgreSQL's core code for UPDATE/DELETE/MERGE operations, handling different relation types with appropriate row identification mechanisms.

## Definition
void add_row_identity_columns(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation)

## Detailed Description
This function is responsible for adding the standard row identity columns that PostgreSQL's executor needs to identify specific rows during UPDATE, DELETE, and MERGE operations. The function handles different types of relations (regular tables, materialized views, partitioned tables, and foreign tables) by adding appropriate row identification variables.

For regular tables, materialized views, and partitioned tables, it adds a "ctid" (row identifier) variable that points to the physical location of the row. For foreign tables, it delegates to the Foreign Data Wrapper (FDW) to add custom row identification columns and may also add a "wholerow" variable for UPDATE operations or when row triggers are present.

The function ensures that the executor has the necessary information to locate and manipulate specific rows, which is particularly important in inheritance hierarchies and partitioned table scenarios where multiple child tables may be involved.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and row-identity variable tracking
- : Range table index of the target relation
- : RangeTblEntry for the target relation
- : Opened relation descriptor for the target table

## Dependencies
- Functions called/Symbols referenced:
  - makeVar: Creates Var nodes for ctid and wholerow variables
  - [add_row_identity_var](add_row_identity_var.md): Registers the created row identity variables
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md): Retrieves FDW routines for foreign tables
  - SelfItemPointerAttributeNumber: System attribute number for ctid
  - InvalidAttrNumber: Used for wholerow variables

- Called from (representative examples):
  - [preprocess_targetlist](../p/preprocess_targetlist.md): During target list preprocessing for DML operations
  - [distribute_row_identity_vars](../d/distribute_row_identity_vars.md): When distributing row identity variables across inheritance hierarchy
  - [expand_single_inheritance_child](../e/expand_single_inheritance_child.md): During inheritance expansion

## Notes and Other Information
- Only processes UPDATE, DELETE, and MERGE commands (enforced by assertion)
- For regular tables/materialized views/partitioned tables: adds "ctid" system column for row identification
- For foreign tables: delegates to FDW's AddForeignUpdateTargets callback and may add "wholerow" variable
- The "wholerow" variable is added for foreign tables in UPDATE operations or when row triggers exist, allowing the executor to access the complete old row
- Foreign tables require special handling because they don't have physical ctid values and may need custom row identification mechanisms
- The function works in conjunction with add_row_identity_var to maintain a global registry of row identity variables
- This is a core component of PostgreSQL's support for DML operations on inheritance hierarchies and partitioned tables