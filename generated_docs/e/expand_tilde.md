# expand_tilde

## Location
src/bin/psql/common.c: 2173 - 2230

## Overview
This function expands tilde (~) characters in file paths to their corresponding home directory paths, supporting both current user (~) and specific user (~username) expansions.

## Definition


## Detailed Description
The  function performs Unix-style tilde expansion on file paths. It substitutes '~' at the beginning of a path with the user's home directory, and '~username' with the specified user's home directory. This functionality is commonly found in Unix shells and provides a convenient way to reference home directories in file paths.

The function handles several cases:
1.  or  - expands to the current user's home directory
2.  - expands to the specified user's home directory (obtained via )
3. Non-tilde paths - left unchanged

The function modifies the input string in place by allocating a new string with the expanded path and freeing the original. On Windows platforms, this function does nothing as Windows doesn't typically use tilde expansion.

## Parameters / Member Variables
- : Double pointer to the filename string to be expanded. The original string may be freed and replaced with an expanded version.

## Dependencies
- Functions called/Symbols referenced:
  - get_home_path (for getting current user's home directory)
  - getpwnam (POSIX function to get user information)
  - strlcpy (safe string copy function)
  - [psprintf](../p/psprintf.md) (PostgreSQL string formatting function)
  - free (memory deallocation)
- Called from (representative examples):
  - [exec_command_edit](exec_command_edit.md) (for \e command file paths)
  - [exec_command_include](exec_command_include.md) (for \i command file paths)  
  - [exec_command_out](exec_command_out.md) (for \o command file paths)
  - [parse_slash_copy](../p/parse_slash_copy.md) (for COPY command file paths)
  - [process_psqlrc](../p/process_psqlrc.md) (for .psqlrc file paths)

## Notes and Other Information
- This function is disabled on WIN32 platforms due to different file path conventions
- The function performs in-place modification of the filename pointer
- Memory management: frees the original string and allocates a new one if expansion occurs
- Only expands tildes at the beginning of the path (not embedded tildes)
- Uses POSIX  function to look up user information
- Critical for psql's file handling commands to support convenient home directory references
- The function gracefully handles cases where user lookup fails by leaving the path unchanged