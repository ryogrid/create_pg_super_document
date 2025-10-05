# remove_variables

## Location
[src/interfaces/ecpg/preproc/variable.c:289-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L289-L366)

## Overview
Removes all variables from the global variable list that were defined at or deeper than the specified brace level, ensuring proper cleanup of cursor references before deallocation.

## Definition

```c
struct variable *p,
			   *prev,
			   *next;
```
## Detailed Description
This function implements comprehensive scope-based cleanup for variables in the ECPG preprocessor. It performs a two-phase cleanup process:

1. **Cursor Reference Cleanup**: Before removing any variable, it traverses all active cursors and removes references to the variable from both  and  argument lists. This prevents dangling pointers when variables go out of scope.

2. **Variable Removal**: After ensuring no cursors reference the variable, it removes the variable from the global  list and performs proper memory cleanup using  for complex types.

This function is critical for maintaining proper scoping semantics and preventing memory leaks when exiting code blocks. The careful handling of cursor references ensures that embedded SQL cursors don't reference variables that have gone out of scope.

## Parameters / Member Variables
- : The minimum brace level threshold; variables at this level or deeper will be removed

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGfree_type](../E/ECPGfree_type.md) (frees ECPG type structures)
  - free (C library function for memory deallocation)
  - allvariables (global variable list)
  - cur (global cursor list)
  - [cursor](../c/cursor.md) (cursor structure type)
  - [arguments](../a/arguments.md) (argument structure type)

- Called from (representative examples):
  - (No direct callers found in the current analysis, likely called during scope exit processing)

## Notes and Other Information
- This is a public function, accessible from other ECPG modules
- Performs critical referential integrity maintenance by cleaning cursor references
- Uses nested loops to traverse cursors and their argument lists
- Handles both  and  cursor argument lists
- Essential for proper memory management and scope semantics in ECPG
- The brace_level parameter typically corresponds to C block nesting depth
- More complex than  due to the need to handle cursor references
- Prevents variable pollution across scope boundaries and cursor corruption

## Simplified Source

```c
void
remove_variables(int brace_level)
{
    struct variable *p, *prev, *next;

    // Traverse all variables in the global list
    for (p = allvariables, prev = NULL; p; p = next) {
        next = p->next;

        if (p->brace_level >= brace_level) {
            // Remove cursor references before deleting variable
            struct cursor *cursor_ptr;
            for (cursor_ptr = cur; cursor_ptr != NULL; cursor_ptr = cursor_ptr->next) {
                // Clean insert arguments list
                remove_variable_from_cursor_args(&cursor_ptr->argsinsert, p);
                // Clean result arguments list
                remove_variable_from_cursor_args(&cursor_ptr->argsresult, p);
            }

            // Remove variable from global list
            if (prev)
                prev->next = next;
            else
                allvariables = next;

            // Free memory
            ECPGfree_type(p->type);
            free(p->name);
            free(p);
        }
        else {
            prev = p;
        }
    }
}

// Helper function to remove variable references from cursor argument lists
static void
remove_variable_from_cursor_args(struct arguments **args_list, struct variable *target_var)
{
    struct arguments *arg_ptr, *prev_arg, *next_arg;

    for (arg_ptr = *args_list, prev_arg = NULL; arg_ptr != NULL; arg_ptr = next_arg) {
        next_arg = arg_ptr->next;

        if (arg_ptr->variable == target_var) {
            // Remove from list
            if (prev_arg)
                prev_arg->next = next_arg;
            else
                *args_list = next_arg;
        }
        else {
            prev_arg = arg_ptr;
        }
    }
}
```