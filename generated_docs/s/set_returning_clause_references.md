# set_returning_clause_references

## Location
src/backend/optimizer/plan/setrefs.c: 3317 - 3360

## Overview
Fixes variable references in RETURNING targetlists for queries involving multiple tables, ensuring proper variable mapping while preserving result-table variable references for executor evaluation.

## Definition


## Detailed Description
The  function performs reference fixing specifically for RETURNING clauses in SQL statements that modify data (INSERT, UPDATE, DELETE). This function is critical for handling complex queries where the RETURNING clause references both the target table and other tables involved in the query.

The function implements a sophisticated strategy:

1. **Selective Variable Handling**: Variables referring to the result table are preserved with their original variable numbers (adjusted by rtoffset) because the executor will evaluate them using the actual heap tuple after firing any triggers.

2. **Non-Result Table Variables**: Variables referencing other tables are converted to reference junk target list entries in the top subplan's target list, using OUTER_VAR as their variable number.

3. **PlaceHolderVar Processing**: The function also handles PlaceHolderVars by searching for them in the target list, though it notes that PlaceHolderVars cannot refer to result relations due to outer join constraints.

4. **Reuse of Existing Machinery**: The function cleverly reuses the  infrastructure that was originally designed for inner index scan fixup, demonstrating efficient code reuse within PostgreSQL's architecture.

The function also performs necessary opcode lookup operations and updates the global relation OID tracking.

## Parameters / Member Variables
- : PlannerInfo structure for the parent query level (not the subplan level)
- : The RETURNING target list that needs to have its variable references fixed
- : The top subplan node that will be positioned just below the ModifyTable node (not yet processed by set_plan_refs)
- : Range table index of the result relation being modified (not yet adjusted by rtoffset)
- : Amount to increment variable numbers by for range table adjustment

## Dependencies
- Functions called/Symbols referenced:
  - build_tlist_index_other_vars (builds indexed target list excluding result relation)
  - fix_join_expr (performs the actual variable reference fixing)
  - pfree (memory cleanup)
  - NRM_EQUAL (nulling-resilient matching mode)
  - NUM_EXEC_TLIST (execution count estimation macro)
- Called from (representative examples):
  - fix_scan_list
  - set_plan_refs

## Notes and Other Information
- This function is part of PostgreSQL's plan reference fixing system for handling RETURNING clauses in DML operations
- The function includes extensive comments explaining the design rationale and constraints
- It demonstrates PostgreSQL's approach to code reuse by leveraging existing join expression fixing machinery
- The function handles a special case where the root parameter refers to the parent query level rather than matching the subplan
- Memory management is carefully handled with proper cleanup of the indexed target list
- The function specifically notes that PlaceHolderVars cannot refer to result relations due to outer join semantics
- Static scope indicates this function is internal to the setrefs.c module
- The design accounts for trigger execution timing, ensuring result-table variables are evaluated correctly