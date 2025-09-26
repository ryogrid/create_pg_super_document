# get_variable

## Location
[src/backend/utils/adt/ruleutils.c:7330-7602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7330-L7602)

## Overview
Displays a Var (variable reference) appropriately in the context of SQL rule decompilation, handling various special cases including whole-row variables, join aliases, and subplan references.

## Definition

```c
struct */
		if (attnum > colinfo->num_cols)
			elog(ERROR, "invalid attnum %d for relation \"%s\"",
				 attnum, rte->eref->aliasname);
```
## Detailed Description
This function is a core component of PostgreSQL's rule decompilation system that converts internal Var nodes back into readable SQL text. It handles the complex task of determining how to display variable references, taking into account nesting levels, join contexts, inheritance hierarchies, and various special cases.

The function performs several key operations:
1. Resolves the appropriate nesting depth using varlevelsup and levelsup parameters
2. Chooses between syntactic and semantic referents based on context
3. Handles special variable numbers (OUTER_VAR, INNER_VAR, INDEX_VAR) by delegating to resolve_special_varno
4. Maps child variables to parent relations when dealing with inheritance
5. Handles resjunk elements in subqueries by drilling down to subplan expressions
6. Processes unnamed join aliases by recursively expanding alias variables
7. Determines whether table prefixes are needed to avoid ambiguity
8. Formats the final output with appropriate quoting and type casting

## Parameters / Member Variables
- : The Var node to be displayed, containing variable number, attribute number, and type information
- : Additional nesting level offset to interpret varlevelsup relative to a context above the current one
- : Flag indicating if this Var appears at the top level of a SELECT targetlist, requiring special whole-row handling
- : Deparse context containing namespace information, buffer for output, and various formatting options

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - rt_fetch
  - [bms_is_member](../b/bms_is_member.md)
  - deparse_columns_fetch
  - [resolve_special_varno](../r/resolve_special_varno.md)
  - [get_special_variable](get_special_variable.md)
  - [get_tle_by_resno](get_tle_by_resno.md)
  - [push_child_plan](../p/push_child_plan.md)/pop_child_plan
  - [get_rule_expr](get_rule_expr.md)
  - [get_rte_attribute_name](get_rte_attribute_name.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
- Called from (representative examples):
  - [get_target_list](get_target_list.md)
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md)
  - [get_rule_expr](get_rule_expr.md)
  - [get_rule_expr_toplevel](get_rule_expr_toplevel.md)

## Notes and Other Information
- Returns the attribute name of the Var, or NULL if the Var has no attname (whole-row Vars or subplan references)
- Uses a "dirty hack" for top-level whole-row Vars, printing "tab.*::typename" instead of "tab.*" to prevent unwanted expansion
- Handles inheritance mapping by walking up the AppendRelInfo chain to find appropriate parent relations
- Contains recursive call to itself when processing join alias variables
- Includes special logic for ORDER BY clauses to add table prefixes when needed to avoid ambiguity with SELECT list items
- Critical for maintaining SQL standard compliance and readability in rule decompilation