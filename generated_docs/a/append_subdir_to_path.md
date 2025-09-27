# append_subdir_to_path

## Location
[src/port/path.c:1124-1133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L1124-L1133)

## Overview
Safely appends a subdirectory name to an output path during path canonicalization, handling overlapping memory regions correctly.

## Definition

```c
static char *
append_subdir_to_path(char *path, char *subdir)
```
## Detailed Description
This function is specifically designed for use within  operations to append subdirectory names to the output path. It handles the delicate operation of copying potentially overlapping memory regions by using  instead of , which is essential since canonicalize_path updates paths in-place.

The function includes an optimization that avoids unnecessary copying when the path and subdir pointers refer to the same memory location. It returns a pointer to the new end location of the path, which is useful for continuing path construction operations. Note that this function does not null-terminate the resulting string, as that responsibility belongs to the calling function.

## Parameters / Member Variables
- : Output buffer where the subdirectory name will be appended
- : Source string containing the subdirectory name to append

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - uses standard C library functions strlen, memmove)
- Called from (representative examples):
  - [canonicalize_path_enc](../c/canonicalize_path_enc.md) (multiple call sites)

## Notes and Other Information
- This is a static function, only available within src/port/path.c
- Returns a pointer to the new end of the path string
- Uses memmove() for safe copying of potentially overlapping memory regions
- Does not null-terminate the resulting string
- Includes optimization to avoid unnecessary copying when path == subdir
- Specifically designed for in-place path manipulation during canonicalization
- Essential component of the path canonicalization process in PostgreSQL
- The returned pointer is typically used for further path construction operations

## Simplified Source

```c
// Simplified version of append_subdir_to_path
static char *append_subdir_to_path(char *path, char *subdir) {
    // Get the length of the subdirectory name
    size_t len = strlen(subdir);

    // Only copy if source and destination are different
    if (path != subdir) {
        // Use memmove for safe overlapping memory copy
        memmove(path, subdir, len);
    }

    // Return pointer to new end of path (for continued construction)
    return path + len;
}
```

Key simplifications made:
- Added clear comments explaining the memory copy logic
- Preserved the essential overlap safety with memmove
- Maintained the optimization for identical pointers
- Kept the important return value for path construction