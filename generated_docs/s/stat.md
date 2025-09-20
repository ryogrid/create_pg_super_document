# stat

## Location
[src/include/port/win32_port.h:262-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/win32_port.h#L262-L280)

## Overview
The  symbol is a preprocessor macro redefinition for Windows compatibility that redirects the standard POSIX  function to Microsoft's native implementation.

## Definition

```c
struct stat						/* This should match struct __stat64 */
{
	_dev_t		st_dev;
	_ino_t		st_ino;
	unsigned short st_mode;
	short		st_nlink;
	short		st_uid;
	short		st_gid;
	_dev_t		st_rdev;
	__int64		st_size;
	__time64_t	st_atime;
	__time64_t	st_mtime;
	__time64_t	st_ctime;
};
```
## Detailed Description
This preprocessor macro is defined in PostgreSQL's Windows port header file to ensure compatibility with Microsoft's native file system stat functionality on Windows platforms. By redefining the standard POSIX  function name to , PostgreSQL can utilize Windows-specific file system operations while maintaining source code compatibility with POSIX systems. This redefinition is crucial for proper file system interaction on Windows, ensuring that file metadata operations work correctly across different operating systems.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - microsoft_native_stat (implicitly through macro expansion)
- Called from (representative examples):
  - This macro is used wherever the standard  function would be called in PostgreSQL code on Windows

## Notes and Other Information
- This macro is specifically defined for Windows compatibility in src/include/port/win32_port.h
- It enables PostgreSQL to use Microsoft's native file system stat functionality seamlessly
- Essential for cross-platform compatibility, allowing the same source code to work on both UNIX-like systems and Windows
- The redirection happens at compile time through preprocessor macro expansion
- Part of PostgreSQL's broader strategy for Windows portability without requiring extensive code changes