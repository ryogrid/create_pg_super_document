# validate_exec

## Location
src/common/exec.c: 88 - 159

## Overview
Validates whether a given path points to an executable file that can be both read and executed, used throughout PostgreSQL to verify program executability before attempting to run external processes.

## Definition


## Detailed Description
The  function performs comprehensive validation of a file path to determine if it represents a valid executable file. It checks for file existence, ensures the file is a regular file (not a directory or device), and verifies that the file has both read and execute permissions. On Windows systems, it automatically appends the ".exe" extension if not present.

The function returns different error codes to distinguish between various failure conditions:
- Returns 0 if the file is valid and executable
- Returns -1 if the file doesn't exist or cannot be executed  
- Returns -2 if the file exists but cannot be read (required for dynamic loading)

On Windows, the function uses file mode bits to check permissions, while on Unix-like systems it uses the  system call for more accurate permission checking.

## Parameters / Member Variables
- : The file path to validate as an executable

## Dependencies
- Functions called/Symbols referenced:
  -  (string copy utility)
  -  (file status system call)
  -  (file accessibility check on Unix)
  -  (macro to test for regular file)
  -  (macro to test for directory)
  -  (user read permission bit on Windows)
  -  (user execute permission bit on Windows)
  -  (read permission constant)
- Called from (representative examples):
  -  (multiple calls for path validation)
  -  (validates found executable paths)
  -  (in pg_upgrade utility)

## Notes and Other Information
- On Windows, automatically handles .exe extension requirements
- Sets appropriate errno values for different failure conditions
- Required for dynamic loading operations which need both read and execute permissions
- Uses different permission checking strategies on Windows vs Unix systems
- Critical for security as it prevents execution of non-regular files like device nodes