# push_child_plan

## Location
src/backend/utils/adt/ruleutils.c: 5076 - 5092

## Overview
A static function that temporarily transfers deparsing attention to a child plan node during expression deparsing operations.

## Definition


## Detailed Description
The  function is used during the deparsing process to temporarily shift focus to a child plan node. This is essential when expanding OUTER_VAR or INNER_VAR references in PostgreSQL's query deparsing functionality. When these variable references are encountered, the deparse context must be adjusted to properly handle cases where the referenced expression itself contains OUTER_VAR/INNER_VAR references.

The function modifies the top stack entry in-place to avoid affecting levelsup issues, which is important for maintaining proper variable reference resolution in Plan trees. It saves the current state, links the current plan into the ancestors list, and then sets attention on the specified child plan.

## Parameters / Member Variables
- : Pointer to the current deparse namespace context that will be modified
- : The child plan node to focus deparsing attention on
- : Local deparse_namespace variable used to save the previous state for later restoration via pop_child_plan

## Dependencies
- Functions called/Symbols referenced:
  - [lcons](../l/lcons.md) (list constructor function)
  - [set_deparse_plan](../s/set_deparse_plan.md) (sets the deparse context for a specific plan)
- Called from (representative examples):
  - [get_variable](../g/get_variable.md)
  - [resolve_special_varno](../r/resolve_special_varno.md)
  - [get_name_for_var_field](../g/get_name_for_var_field.md)

## Notes and Other Information
- This is a static function within ruleutils.c, indicating it's for internal use within the rule/query deparsing subsystem
- Must be paired with  to restore the previous deparse context
- The caller is responsible for providing a local deparse_namespace variable to save state
- Critical for proper handling of nested variable references in complex query plans
- Part of PostgreSQL's query deparsing infrastructure used for rule and view expansion