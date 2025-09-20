# push_ancestor_plan

## Location
[src/backend/utils/adt/ruleutils.c:5123-5143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5123-L5143)

## Overview
A static function that temporarily transfers deparsing attention to an ancestor plan node when expanding parameter references.

## Definition

```c
static void
push_ancestor_plan(deparse_namespace *dpns, ListCell *ancestor_cell,
				   deparse_namespace *save_dpns)
```
## Detailed Description
The  function is used during the deparsing process to temporarily shift focus to an ancestor plan node. This is essential when expanding Param references in PostgreSQL's query deparsing functionality. When a Param reference is encountered, the deparse context must be adjusted to match the plan node that contains the expression being printed. This prevents failures when that expression itself contains Param, OUTER_VAR, INNER_VAR, or INDEX_VAR variables.

The function identifies the target ancestor plan using the ListCell that holds it in the dpns->ancestors list. It builds a new ancestor list containing only the ancestors of the selected node and sets the deparse context to focus on that ancestor plan.

## Parameters / Member Variables
- : Pointer to the current deparse namespace context that will be modified
- : ListCell pointing to the target ancestor plan in the dpns->ancestors list
- : Local deparse_namespace variable used to save the previous state for later restoration via pop_ancestor_plan

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (list access macro)
  - [list_copy_tail](../l/list_copy_tail.md) (copies list elements from a specified position)
  - [list_cell_number](../l/list_cell_number.md) (returns the position number of a list cell)
  - [set_deparse_plan](../s/set_deparse_plan.md) (sets the deparse context for a specific plan)
- Called from (representative examples):
  - [get_name_for_var_field](../g/get_name_for_var_field.md)
  - [get_parameter](../g/get_parameter.md)

## Notes and Other Information
- This is a static function within ruleutils.c for internal use within the rule/query deparsing subsystem
- Must be paired with  to restore the previous deparse context
- The caller is responsible for providing a local deparse_namespace variable to save state
- Critical for proper handling of parameter references in complex nested query plans
- Uses list operations to precisely manage the ancestor chain during context switching
- Part of PostgreSQL's query deparsing infrastructure used for rule and view expansion