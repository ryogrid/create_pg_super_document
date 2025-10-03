# pop_child_plan

## Location
[src/backend/utils/adt/ruleutils.c:5093-5122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5093-L5122)

## Overview
A static function that restores the previous deparse context after temporarily focusing on a child plan node.

## Definition

```c
static void
pop_child_plan(deparse_namespace *dpns, deparse_namespace *save_dpns)
```
## Detailed Description
The  function undoes the effects of  by restoring the previous deparse namespace state. This is the complementary function that must be called after using  to ensure proper cleanup and restoration of the deparsing context. It removes the ancestors list cell that was added by  and restores all fields that were modified during the child plan focus operation.

The function ensures that the deparse context is properly restored to its state before the child plan was pushed, maintaining the integrity of the deparsing operation stack.

## Parameters / Member Variables
- `*dpns`: Pointer to the current deparse namespace context that will be restored
- `*save_dpns`: The saved deparse namespace state that was preserved by
## Dependencies
- Functions called/Symbols referenced:
  - [list_delete_first](../l/list_delete_first.md) (removes the first element from a list)
- Called from (representative examples):
  - [get_variable](../g/get_variable.md)
  - [resolve_special_varno](../r/resolve_special_varno.md)
  - [get_name_for_var_field](../g/get_name_for_var_field.md)

## Notes and Other Information
- This is a static function within ruleutils.c for internal use within the rule/query deparsing subsystem
- Must be used in conjunction with  to maintain proper deparse context stack management
- The function ensures that the ancestors list is correctly maintained even though it may be unnecessary in some cases
- Critical for preventing memory leaks and context corruption in complex query deparsing operations
- Part of PostgreSQL's query deparsing infrastructure used for rule and view expansion

## Simplified Source

```c
static void pop_child_plan(deparse_namespace *dpns, deparse_namespace *save_dpns) {
    // Remove the ancestors list cell added by push_child_plan
    List *ancestors = list_delete_first(dpns->ancestors);

    // Restore all fields modified by push_child_plan
    *dpns = *save_dpns;

    // Ensure ancestors list is correctly maintained
    dpns->ancestors = ancestors;
}
```