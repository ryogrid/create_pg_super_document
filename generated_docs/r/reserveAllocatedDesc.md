# reserveAllocatedDesc

## Location
[src/backend/storage/file/fd.c:2505-2579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2505-L2579)

## Overview
reserveAllocatedDesc is an internal static function that manages memory allocation for the allocatedDescs array, ensuring space is available for tracking allocated file descriptors in PostgreSQL's file descriptor management system.

## Definition

```c
static bool
reserveAllocatedDesc(void)
```
## Detailed Description
reserveAllocatedDesc manages the dynamic allocation and expansion of the allocatedDescs array, which tracks all allocated file descriptors outside of PostgreSQL's Virtual File Descriptor (VFD) system. The function implements a three-tier allocation strategy:

1. **Quick check**: Returns immediately if space is already available in the existing array
2. **Initial allocation**: Creates the initial array with FD_MINFREE/3 elements if it doesn't exist
3. **Array expansion**: Enlarges the array up to max_safe_fds/3 when more space is needed

The function carefully manages memory allocation to prevent excessive consumption of available file descriptors while ensuring adequate space for allocated descriptors. It treats initial allocation failures as fatal errors but handles expansion failures gracefully by returning false.

## Parameters / Member Variables
This function takes no parameters and returns a boolean indicating success or failure.

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDesc (structure type for allocated descriptor tracking)
  - FD_MINFREE (constant defining minimum free file descriptors)
  - malloc (initial memory allocation)
  - realloc (memory expansion)
  - ereport/ERROR (error reporting for fatal memory failures)
- Called from (representative examples):
  - [AllocateFile](../A/AllocateFile.md) (when allocating stdio FILE* descriptors)
  - [OpenTransientFilePerm](../O/OpenTransientFilePerm.md) (for temporary file operations)  
  - [OpenPipeStream](../O/OpenPipeStream.md) (for pipe operations)
  - [AllocateDir](../A/AllocateDir.md) (for directory stream allocation)

## Notes and Other Information
- This is a static function internal to fd.c and not exposed in headers
- Implements a conservative allocation strategy to prevent file descriptor exhaustion
- Initial allocation uses FD_MINFREE/3 elements, expansion limited to max_safe_fds/3
- Memory allocation failures during expansion are non-fatal and return false
- Initial allocation failures are treated as fatal errors requiring process termination
- The function supports PostgreSQL's three-way partitioning of file descriptors: VFD cache, allocated descriptors, and external descriptors
- Part of PostgreSQL's resource management system to ensure stable operation under file descriptor pressure

## Simplified Source

```c
// Simplified version of reserveAllocatedDesc
static bool reserveAllocatedDesc(void) {
    // Quick check: return if space already available
    if (numAllocatedDescs < maxAllocatedDescs)
        return true;

    // Initial allocation: create array if it doesn't exist
    if (allocatedDescs == NULL) {
        int newMax = FD_MINFREE / 3;
        allocatedDescs = malloc(newMax * sizeof(AllocateDesc));
        if (allocatedDescs == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        maxAllocatedDescs = newMax;
        return true;
    }

    // Array expansion: try to enlarge existing array
    int newMax = max_safe_fds / 3;
    if (newMax > maxAllocatedDescs) {
        AllocateDesc *newDescs = realloc(allocatedDescs, newMax * sizeof(AllocateDesc));
        if (newDescs == NULL)
            return false;  // Expansion failed, but non-fatal
        allocatedDescs = newDescs;
        maxAllocatedDescs = newMax;
        return true;
    }

    // Cannot enlarge further
    return false;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic flow
- Consolidated variable declarations closer to usage
- Streamlined error handling paths
- Focused on the three main execution branches
- Preserved the critical difference between fatal initial allocation failure vs non-fatal expansion failure