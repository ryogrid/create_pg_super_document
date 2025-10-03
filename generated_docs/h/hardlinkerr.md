# hardlinkerr

## Location
[src/timezone/zic.c:996-1003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L996-L1003)

## Overview
Creates a hard link from target to linkname, following any symbolic links in the target path, and returns an error code indicating success or failure.

## Definition

```c
static int
hardlinkerr(char const *target, char const *linkname)
```
## Detailed Description
The  function creates a hard link using the POSIX  system call with the  flag. This means that if the target is a symbolic link, the hard link will be created to the file that the symbolic link points to, rather than to the symbolic link itself. The function provides a simple wrapper around  that converts the return value to a more convenient error reporting format.

## Parameters / Member Variables
- `*target`: The path to the existing file to which a hard link should be created
- `*linkname`: The path where the new hard link should be created
## Dependencies
- Functions called/Symbols referenced:
  - linkat (POSIX system call for creating hard links)
- Called from:
  - [dolink](../d/dolink.md) (twice, at lines 1034 and 1039 in src/timezone/zic.c)

## Notes and Other Information
- This is a static function local to src/timezone/zic.c, part of PostgreSQL's timezone handling code
- Returns 0 on success, or the errno value on failure
- Uses AT_FDCWD to specify current working directory for both target and linkname paths
- The AT_SYMLINK_FOLLOW flag ensures that symbolic links in the target are resolved before creating the hard link
- This function is used as a fallback or alternative linking mechanism in the timezone compilation process