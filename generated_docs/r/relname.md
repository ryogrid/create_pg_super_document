# relname

## Location
[src/timezone/zic.c:951-995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L951-L995)

## Overview
Generates a relative path from a target file to a linkname, computing the necessary "../" components to navigate from the linkname directory to the target file.

## Definition

```c
static char *
relname(char const *target, char const *linkname)
```
## Detailed Description
The  function computes a relative path that can be used to reference the target file from the location of the linkname. This is particularly useful for creating relative symbolic links. The function analyzes the common directory prefix between target and linkname paths, then constructs a relative path using "../" sequences to go up the directory hierarchy from linkname's directory to the common ancestor, followed by the remaining path to reach the target.

When the linkname is an absolute path (starts with '/'), the function first converts the target to an absolute path by prepending the global  variable.

## Parameters / Member Variables
- `*target`: The path to the file that should be referenced
- `*linkname`: The path from which the relative reference should be created
## Dependencies
- Functions called/Symbols referenced:
  - [emalloc](../e/emalloc.md) (memory allocation function)
  - strlen (standard C library function)
  - strcpy (standard C library function)
  - memcpy (standard C library function)
  - memmove (standard C library function)
- Global variables accessed:
  - directory (used when linkname is absolute)

## Notes and Other Information
- This is a static function local to src/timezone/zic.c, part of PostgreSQL's timezone handling code
- The function handles both absolute and relative paths for linkname
- Memory is allocated dynamically for the result, which must be freed by the caller
- The algorithm efficiently finds the common directory prefix and calculates the minimum number of "../" sequences needed
- No direct callers were found in the current analysis, suggesting it may be used internally within the same file or module

## Simplified Source

```c
static char *
relname(char const *target, char const *linkname)
{
    size_t dir_len = 0, dotdots = 0;
    char const *f = target;
    char *result = NULL;

    // If linkname is absolute, make target absolute too
    if (*linkname == '/') {
        size_t len = strlen(directory);
        bool needslash = len && directory[len - 1] != '/';

        size_t linksize = len + needslash + strlen(target) + 1;
        f = result = emalloc(linksize);
        strcpy(result, directory);
        result[len] = '/';
        strcpy(result + len + needslash, target);
    }

    // Find common directory prefix
    size_t i;
    for (i = 0; f[i] && f[i] == linkname[i]; i++)
        if (f[i] == '/')
            dir_len = i + 1;

    // Count directory levels to go up from linkname
    for (; linkname[i]; i++)
        dotdots += linkname[i] == '/' && linkname[i - 1] != '/';

    // Build relative path: "../" * dotdots + remaining target path
    size_t taillen = strlen(f + dir_len);
    size_t dotdotetcsize = 3 * dotdots + taillen + 1;

    if (!result)
        result = emalloc(dotdotetcsize);

    for (i = 0; i < dotdots; i++)
        memcpy(result + 3 * i, "../", 3);
    memmove(result + 3 * dotdots, f + dir_len, taillen + 1);

    return result;
}
```