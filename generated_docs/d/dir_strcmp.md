# dir_strcmp

## Location
[src/port/path.c:689-736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L689-L736)

## Overview
A specialized string comparison function for directory paths that treats different directory separators as equivalent and honors filesystem case sensitivity rules.

## Definition

```c
static int
dir_strcmp(const char *s1, const char *s2)
```
## Detailed Description
The  function performs a specialized comparison of two path strings with directory-aware logic. Unlike standard , this function considers any two directory separator characters as equal (e.g., '/' and '\' on Windows), making it suitable for cross-platform path comparisons. 

The function also handles filesystem case sensitivity appropriately:
- On Unix-like systems: Case-sensitive comparison (standard character comparison)
- On Windows: Case-insensitive comparison using 

The comparison continues character by character until a difference is found or one string ends. The return value follows standard comparison semantics: 0 for equal strings, positive if s1 > s2, negative if s1 < s2.

## Parameters / Member Variables
- `*s1`: First path string to compare
- `*s2`: Second path string to compare
## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL's character case conversion function (Windows only)
  -  - Macro to check if a character is a directory separator

- Called from (representative examples):
  -  - Used for path relativization logic

## Notes and Other Information
- This is a static function, only visible within the same source file (src/port/path.c)
- Essential for cross-platform path manipulation where different directory separators may be used
- The case-insensitive behavior on Windows matches the filesystem's native behavior
- Used internally by path manipulation functions to ensure consistent path comparison behavior
- Handles the common scenario where paths might use mixed separator styles (particularly on Windows)

## Simplified Source

```c
// Simplified version of dir_strcmp
static int dir_strcmp(const char *s1, const char *s2) {
    // Compare characters until difference found or string ends
    while (*s1 && *s2) {
        bool chars_different;

#ifndef WIN32
        // Unix: case-sensitive comparison
        chars_different = (*s1 != *s2);
#else
        // Windows: case-insensitive comparison
        chars_different = (pg_tolower((unsigned char) *s1) !=
                          pg_tolower((unsigned char) *s2));
#endif

        // Allow any directory separators to match each other
        if (chars_different && !(IS_DIR_SEP(*s1) && IS_DIR_SEP(*s2))) {
            return (int) *s1 - (int) *s2;
        }

        s1++;
        s2++;
    }

    // Handle different string lengths
    if (*s1) return 1;   // s1 longer
    if (*s2) return -1;  // s2 longer
    return 0;            // equal length
}
```

Key simplifications made:
- Added comments explaining platform-specific behavior
- Extracted the character difference logic for clarity
- Clearly separated the directory separator handling
- Maintained the original comparison semantics