# set_deparse_plan

## Location
[src/backend/utils/adt/ruleutils.c:4965-5045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4965-L5045)

## Overview
Configures a deparse_namespace structure to handle expression parsing for a specific Plan node by setting up appropriate outer, inner, and index target list references based on the plan type.

## Definition
```c
static void set_deparse_plan(deparse_namespace *dpns, Plan *plan)
```

## Detailed Description
This function is a critical component of PostgreSQL's rule decompilation system that configures the deparse namespace for parsing subexpressions within a given Plan node. It establishes the context for variable resolution by setting up the outer_plan, inner_plan, and various target list references based on the specific type and characteristics of the plan node.

The function handles numerous special cases for different plan types:
- For Append and MergeAppend plans, it treats the first child plan as the OUTER referent
- For SubqueryScan, it uses the subplan as the INNER referent
- For CteScan, it locates the corresponding CTE subplan as INNER referent
- For WorkTableScan, it finds the parent RecursiveUnion plan as INNER referent
- For ModifyTable with MERGE operations, it uses the source plan as INNER referent
- For various scan types (IndexOnlyScan, ForeignScan, CustomScan), it sets up INDEX_VAR target lists

This setup is essential for proper variable resolution during expression decompilation, ensuring that OUTER_VAR, INNER_VAR, and INDEX_VAR references are correctly interpreted in the context of the specific plan structure.

## Parameters / Member Variables
- `dpns`: Pointer to deparse_namespace structure to be configured with plan-specific information
- `plan`: Pointer to the Plan node that will serve as the context for expression parsing

## Dependencies
- Functions called/Symbols referenced:
  - deparse_namespace (namespace structure)
  - [Plan](../P/Plan.md) (base plan node structure)
  - Append, MergeAppend (append-type plan nodes)
  - SubqueryScan, CteScan, WorkTableScan (scan plan nodes)
  - [ModifyTable](../M/ModifyTable.md) (modification plan node)
  - IndexOnlyScan, ForeignScan, CustomScan (specialized scan nodes)
  - outerPlan, innerPlan (macros to access child plans)
  - linitial (macro to get first list element)
  - [list_nth](../l/list_nth.md) (function to get nth list element)
  - [find_recursive_union](../f/find_recursive_union.md) (function to locate recursive union plans)
  - CMD_MERGE, CMD_INSERT (command type constants)
- Called from (representative examples):
  - [set_deparse_context_plan](set_deparse_context_plan.md) (at line 3786)
  - [push_child_plan](../p/push_child_plan.md) (at line 5086)
  - [push_ancestor_plan](../p/push_ancestor_plan.md) (at line 5137)

## Notes and Other Information
- This is a static function, only accessible within ruleutils.c
- The function assumes the caller has already adjusted the ancestors list in the deparse namespace
- Does not modify rtable, subplans, or ctes fields as these remain constant within a single plan tree
- Handles complex plan type hierarchies and special cases for variable resolution
- Essential for maintaining proper scoping and variable resolution during rule decompilation
- The target list assignments enable correct interpretation of Var nodes with different varnos (OUTER_VAR, INNER_VAR, INDEX_VAR)
- Part of the broader plan decompilation infrastructure that converts execution plans back to SQL text