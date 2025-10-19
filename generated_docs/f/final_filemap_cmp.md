# final_filemap_cmp

## Location
[src/bin/pg_rewind/filemap.c:680-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L680-L699)

## Overview
A comparison function used by qsort to order file entries for pg_rewind operations, ensuring that file actions are executed in the proper order for safety and correctness.

## Definition

```c
static int
final_filemap_cmp(const void *a, const void *b)
```
## Detailed Description
This function implements the comparison logic for sorting file entries in the final stage of pg_rewind processing. The sorting strategy prioritizes safety over disk space efficiency by:

1. **Action-based ordering**: Primary sort by the file_action_t enum values, which are deliberately ordered so that creation operations come before removal operations
2. **Path-based ordering**: Secondary sort by file path, with special handling for removal actions
3. **Removal safety**: For FILE_ACTION_REMOVE entries, uses reverse path order so that subdirectories are removed before their parent directories (e.g., "foo/bar" before "foo")

The function ensures that:
- Parent directories are created before their contents
- Directory contents are removed before the directories themselves  
- Operations are performed in a predictable, safe order

## Parameters / Member Variables
- `*a`: Pointer to first file_entry_t pointer to compare
- `*b`: Pointer to second file_entry_t pointer to compare
## Dependencies
- Functions called/Symbols referenced:
  - [file_entry_t](file_entry_t.md) (struct)
  - FILE_ACTION_REMOVE (enum value)  
  - strcmp (standard library function)
- Called from (representative examples):
  - [decide_file_actions](../d/decide_file_actions.md) (via qsort)

## Notes and Other Information
- This is a static function internal to filemap.c
- The function follows the standard qsort comparison function contract: returns negative, zero, or positive integer for less than, equal to, or greater than relationships
- The enum ordering in file_action_t is crucial for this function to work correctly
- Safety is prioritized over disk space efficiency - removals come last even though doing them first would be more space-efficient
- The reverse ordering for removal paths ensures proper cleanup of nested directory structures

## Simplified Source

```c
static int
final_filemap_cmp(const void *a, const void *b)
{
    file_entry_t *fa = *((file_entry_t **) a);
    file_entry_t *fb = *((file_entry_t **) b);

    // Primary sort: by action type (create before remove)
    if (fa->action > fb->action)
        return 1;
    if (fa->action < fb->action)
        return -1;

    // Secondary sort: by path
    // For removals, use reverse order (subdirs before parent dirs)
    if (fa->action == FILE_ACTION_REMOVE)
        return strcmp(fb->path, fa->path);
    else
        return strcmp(fa->path, fb->path);
}
```