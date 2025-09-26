# examine_variable

## Location
[src/backend/utils/adt/selfuncs.c:5025-5350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5025-L5350)

## Overview
Analyzes an expression tree to extract statistical information and fill a VariableStatData structure with details about the variable, its type, uniqueness, and associated statistics.

## Definition

```c
void
examine_variable(PlannerInfo *root, Node *node, int varRelid,
				 VariableStatData *vardata)
```
## Detailed Description
This is a central function in PostgreSQL's selectivity estimation system that performs comprehensive analysis of expressions to gather statistical information. The function handles various types of expressions, from simple column references (Vars) to complex expressions involving multiple relations.

The function follows a multi-layered approach: it first handles simple Var nodes as a fast path, then proceeds to analyze more complex expressions by determining their variable membership across relations. For expressions that reference columns from a single relation, it attempts to find matching statistics either from regular column statistics, expressional indexes, or extended statistics objects.

The function is particularly sophisticated in handling expressional indexes and extended statistics. When an expression matches an index expression, it retrieves statistics from the index and determines if the expression represents a unique value. For extended statistics, it searches through statistics objects that contain per-expression statistics and loads the appropriate statistical data.

The function also handles security considerations by checking whether the current user has permission to access the underlying table data, which affects whether certain statistical information can be used safely.

## Parameters / Member Variables
- : Pointer to PlannerInfo structure containing planner context and query information
- : The expression tree to be analyzed for statistical information
- : Relation ID for restriction context; when nonzero, only variables from this relation are considered as variables
- : Output parameter that gets filled with comprehensive information about the variable including statistics, type information, uniqueness, and access permissions

## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - exprType
  - exprTypmod
  - IsA
  - find_base_rel
  - find_join_rel
  - has_unique_index
  - examine_simple_variable
  - pull_varnos
  - bms_difference
  - bms_is_empty
  - bms_get_singleton_member
  - bms_is_member
  - bms_overlap
  - bms_free
  - remove_nulling_relids
  - list_head
  - lnext
  - equal
  - SearchSysCache3
  - HeapTupleIsValid
  - ReleaseSysCache
  - all_rows_selectable
  - statext_expressions_load
  - ReleaseDummy
  - planner_rt_fetch
- Called from (representative examples):
  - get_restriction_variable
  - get_join_variables
  - boolvarsel
  - booltestsel
  - nulltestsel
  - estimate_array_length
  - mergejoinscansel
  - estimate_num_groups
  - estimate_hash_bucket_stats
  - scalararraysel_containment

## Notes and Other Information
- The function initializes vardata with MemSet to ensure no dangling pointers are returned
- For simple Var nodes, it provides a fast path that directly retrieves column statistics and uniqueness information
- The function strips binary-compatible relabeling to work with the underlying expression structure
- It differentiates between base relations and join relations when analyzing variable membership
- For expressional indexes, it searches through all index expressions to find matches and retrieves corresponding statistics
- Extended statistics support allows the function to find statistics for complex expressions that aren't covered by regular column or index statistics
- Security checks ensure that statistical information is only used when the current user has appropriate table access permissions
- The caller is responsible for calling ReleaseVariableStats() to clean up any allocated statistical data
- The function handles inheritance hierarchies correctly by checking permissions on the appropriate parent relation