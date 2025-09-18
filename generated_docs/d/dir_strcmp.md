# dir_strcmp

## Location
[src/port/path.c:689-736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L689-L736)

## Overview
A specialized string comparison function for directory paths that treats different directory separators as equivalent and honors filesystem case sensitivity rules.

## Definition


## Detailed Description
The  function performs a specialized comparison of two path strings with directory-aware logic. Unlike standard , this function considers any two directory separator characters as equal (e.g., '/' and '\' on Windows), making it suitable for cross-platform path comparisons. 

The function also handles filesystem case sensitivity appropriately:
- On Unix-like systems: Case-sensitive comparison (standard character comparison)
- On Windows: Case-insensitive comparison using 

The comparison continues character by character until a difference is found or one string ends. The return value follows standard comparison semantics: 0 for equal strings, positive if s1 > s2, negative if s1 < s2.

## Parameters / Member Variables
- : First path string to compare
- : Second path string to compare

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