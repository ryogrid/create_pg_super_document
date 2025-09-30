# pop_ancestor_plan

## Location
[src/backend/utils/adt/ruleutils.c:5144-5159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5144-L5159)

## Overview
A static function that restores the previous deparse context after temporarily focusing on an ancestor plan node.

## Definition

```c
static void
pop_ancestor_plan(deparse_namespace *dpns, deparse_namespace *save_dpns)
```
## Detailed Description
The  function undoes the effects of  by restoring the previous deparse namespace state. This is the complementary function that must be called after using  to ensure proper cleanup and restoration of the deparsing context. It frees the ancestor list that was created by  and restores all fields that were modified during the ancestor plan focus operation.

The function ensures that the deparse context is properly restored to its state before the ancestor plan was pushed, maintaining the integrity of the deparsing operation stack and preventing memory leaks.

## Parameters / Member Variables
- : Pointer to the current deparse namespace context that will be restored
- : The saved deparse namespace state that was preserved by 

## Dependencies
- Functions called/Symbols referenced:
  - [list_free](../l/list_free.md) (frees memory allocated for a list)
- Called from (representative examples):
  - [get_name_for_var_field](../g/get_name_for_var_field.md)
  - [get_parameter](../g/get_parameter.md)

## Notes and Other Information
- This is a static function within ruleutils.c for internal use within the rule/query deparsing subsystem
- Must be used in conjunction with  to maintain proper deparse context stack management
- The function properly frees the copied ancestor list to prevent memory leaks
- Critical for maintaining memory management discipline in complex query deparsing operations
- Simpler than  because it doesn't need to preserve existing ancestor list elements
- Part of PostgreSQL's query deparsing infrastructure used for rule and view expansion

## Simplified Source

```c
static void pop_ancestor_plan(deparse_namespace *dpns, deparse_namespace *save_dpns) {
    // Free the ancestor list created by push_ancestor_plan
    list_free(dpns->ancestors);

    // Restore all fields modified by push_ancestor_plan
    *dpns = *save_dpns;
}
```