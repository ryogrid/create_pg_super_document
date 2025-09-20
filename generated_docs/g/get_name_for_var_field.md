# get_name_for_var_field

## Location
[src/backend/utils/adt/ruleutils.c:7732-8161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7732-L8161)

## Overview
Determines the field name of a specified field within an expression of composite type, handling complex cases including RECORD types, special variables, subqueries, and CTEs through recursive analysis.

## Definition

```c
static const char *
get_name_for_var_field(Var *var, int fieldno,
					   int levelsup, deparse_context *context)
```
## Detailed Description
This function handles the complex task of determining field names for composite type expressions during rule decompilation. It deals with several challenging scenarios:

1. **RowExpr handling**: For RowExpr nodes expanded from whole-row Vars, directly extracts column names from the attached colnames list.

2. **RECORD type resolution**: When encountering RECORD types (which can't exist in actual table columns), the function drills down through various expression types to find the ultimate source and infer the field name.

3. **Parameter resolution**: For Param nodes of type RECORD, uses find_param_referent to locate the actual referenced expression and recursively processes it.

4. **Special variable handling**: Manages OUTER_VAR, INNER_VAR, and INDEX_VAR references by traversing into subplans and recursively calling itself on the resolved expressions.

5. **Complex RTE types**: Handles various range table entry types including:
   - Subqueries: Examines target list entries and may recurse into sub-select queries
   - Joins: Follows joinaliasvars to find the actual source expression  
   - CTEs: Locates the referenced CTE and examines its target list
   - Relations/Values/etc.: Generally should not occur for RECORD types

6. **Plan tree compatibility**: Includes special handling for plan trees where some information (like subquery details) may not be available, falling back to generic names like "f1", "f2", etc.

The function maintains proper namespace context throughout recursive calls and handles inheritance mapping appropriately.

## Parameters / Member Variables
- : The expression (typically a Var) whose field name is needed
- : The 1-based field number within the composite type
- : Additional nesting level offset for interpreting varlevelsup
- : Deparse context containing namespace stack and other formatting state

## Dependencies
- Functions called/Symbols referenced:
  - [find_param_referent](../f/find_param_referent.md) (for PARAM resolution)
  - [get_expr_result_tupdesc](get_expr_result_tupdesc.md) (for final tuple descriptor extraction)
  - [get_tle_by_resno](get_tle_by_resno.md) (target list entry retrieval)
  - [push_child_plan](../p/push_child_plan.md)/pop_child_plan (context management)
  - [push_ancestor_plan](../p/push_ancestor_plan.md)/pop_ancestor_plan (parameter context handling)
  - [set_deparse_for_query](../s/set_deparse_for_query.md) (subquery namespace setup)
  - [get_rte_attribute_name](get_rte_attribute_name.md) (system column names)
  - GetCTETargetList (CTE target list access)
  - [get_name_for_var_field](get_name_for_var_field.md) (recursive self-calls)
- Called from (representative examples):
  - get_rule_expr (for FieldSelect expressions)
  - [get_name_for_var_field](get_name_for_var_field.md) (recursive calls)

## Notes and Other Information
- Returns const char* pointing to the field name, which may be allocated in various memory contexts depending on the resolution path
- The function is heavily recursive and includes stack depth protection through check_stack_depth calls in called functions
- Handles both parse tree and plan tree contexts, with different logic paths for each
- When field names cannot be determined (empty subqueries/CTEs), falls back to generic "fN" naming convention
- Critical for proper handling of composite types in complex queries involving joins, subqueries, and CTEs
- The logic parallels the parser's expandRecordVariable() function but operates in the reverse direction during decompilation
- Includes extensive error checking for bogus variable references and missing target list entries