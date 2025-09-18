# pop_child_plan

## Location
src/backend/utils/adt/ruleutils.c: 5093 - 5122

## Overview
A static function that restores the previous deparse context after temporarily focusing on a child plan node.

## Definition


## Detailed Description
The  function undoes the effects of  by restoring the previous deparse namespace state. This is the complementary function that must be called after using  to ensure proper cleanup and restoration of the deparsing context. It removes the ancestors list cell that was added by  and restores all fields that were modified during the child plan focus operation.

The function ensures that the deparse context is properly restored to its state before the child plan was pushed, maintaining the integrity of the deparsing operation stack.

## Parameters / Member Variables
- : Pointer to the current deparse namespace context that will be restored
- : The saved deparse namespace state that was preserved by 

## Dependencies
- Functions called/Symbols referenced:
  - list_delete_first (removes the first element from a list)
- Called from (representative examples):
  - get_variable
  - resolve_special_varno
  - get_name_for_var_field

## Notes and Other Information
- This is a static function within ruleutils.c for internal use within the rule/query deparsing subsystem
- Must be used in conjunction with  to maintain proper deparse context stack management
- The function ensures that the ancestors list is correctly maintained even though it may be unnecessary in some cases
- Critical for preventing memory leaks and context corruption in complex query deparsing operations
- Part of PostgreSQL's query deparsing infrastructure used for rule and view expansion