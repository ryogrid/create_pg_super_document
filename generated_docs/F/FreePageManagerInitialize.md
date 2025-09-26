# FreePageManagerInitialize

## Location
[src/backend/utils/mmgr/freepage.c:183-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L183-L209)

## Overview
Initializes a new, empty free page manager structure with default values and sets up relative pointers for shared memory contexts.

## Definition

```c
void
FreePageManagerInitialize(FreePageManager *fpm, char *base)
```
## Detailed Description
FreePageManagerInitialize sets up a FreePageManager structure for tracking free pages in either dynamic shared memory segments or backend-private memory. The function initializes all internal structures to empty/default states and configures relative pointers that allow the manager to work correctly in shared memory contexts where absolute addresses may vary between processes.

The initialization process:
1. Sets up the self-reference pointer using the provided base address
2. Initializes the B-tree root and recycling structures to NULL
3. Resets counters and depth tracking to zero
4. Clears singleton page tracking
5. Marks contiguous page tracking as dirty to force recalculation
6. Initializes all freelists to empty

## Parameters / Member Variables
- : Pointer to caller-provided FreePageManager structure to initialize
- : Base address for relative pointer calculations; typically the start of a dynamic shared memory segment or NULL/start of extent for backend-private memory

## Dependencies
- Functions called/Symbols referenced:
  - relptr_store (for setting up relative pointers)
- Types referenced:
  - FreePageManager
  - FreePageBtree
  - FreePageSpanLeader
  - FPM_NUM_FREELISTS
- Called from (representative examples):
  - dsm_shmem_init
  - create_internal (DSA)
  - make_new_segment (DSA)
  - fpm_largest

## Notes and Other Information
- The function assumes the caller has allocated sufficient memory for the FreePageManager structure
- All relative pointers are properly initialized to work in shared memory contexts
- The contiguous_pages_dirty flag is set to true to ensure proper recalculation on first access
- Debug builds include additional free_pages tracking via FPM_EXTRA_ASSERTS
- This is typically the first function called when setting up free page management for a memory segment