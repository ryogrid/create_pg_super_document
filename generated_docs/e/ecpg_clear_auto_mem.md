# ecpg_clear_auto_mem

## Location
[src/interfaces/ecpg/ecpglib/memory.c:151-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L151-L167)

## Overview
Clears the automatic memory management tracking structures without freeing the user data pointers tracked by the ECPG library.

## Definition
```c
void ecpg_clear_auto_mem(void)
```

## Detailed Description
This function is part of ECPG's automatic memory management system and serves as a selective cleanup mechanism. Unlike `ECPGfree_auto_mem` which frees both the tracked memory and the tracking structures, `ecpg_clear_auto_mem` only frees the `auto_mem` tracking structures themselves while leaving the user data pointers intact.

The function operates by:
1. Retrieving the current thread's auto-allocation list via `get_auto_allocs()`
2. Iterating through each `auto_mem` node in the linked list
3. Freeing only the `auto_mem` structure itself (not the user data pointer)
4. Resetting the thread's auto-allocation list to NULL

This selective cleanup is useful when the user data should remain valid but the automatic tracking needs to be reset, typically during connection management or transaction boundaries.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [get_auto_allocs](../g/get_auto_allocs.md)
  - [auto_mem](../a/auto_mem.md) (struct)
  - [ecpg_free](ecpg_free.md)
  - [set_auto_allocs](../s/set_auto_allocs.md)
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md)
  - [var_list](../v/var_list.md)
  - [ecpg_do_prologue](ecpg_do_prologue.md)

## Notes and Other Information
- This function differs from `ECPGfree_auto_mem` in that it does NOT free the user data pointers (`act->pointer`)
- Used during connection establishment and SQL execution preparation where tracking structures need to be reset but user data remains valid
- The function is thread-safe as it operates on thread-local storage via pthread-specific keys
- Commonly called in prologue functions and connection management to prepare for new operations
- The comment "only free our own structure" in the source code emphasizes this selective behavior
- Essential for preventing double-free errors when user data lifetime extends beyond the tracking period

## Simplified Source

```c
void
ecpg_clear_auto_mem(void)
{
    struct auto_mem *am = get_auto_allocs();

    // Clear only tracking structures, not user data
    if (am) {
        do {
            struct auto_mem *act = am;
            am = am->next;
            ecpg_free(act);  // Free tracking structure only
        } while (am);

        set_auto_allocs(NULL);
    }
}
```