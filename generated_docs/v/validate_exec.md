# validate_exec

## Location
[src/common/exec.c:88-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L88-L159)

## Overview
Validates whether a given path points to an executable file that can be both read and executed, used throughout PostgreSQL to verify program executability before attempting to run external processes.

## Definition

```c
struct stat buf;
```
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

## Simplified Source

```c
// Simplified version of validate_exec
int validate_exec(const char *path) {
    struct stat buf;
    const char *exec_path = path;

#ifdef WIN32
    // Windows: Add .exe extension if missing
    char path_exe[MAXPGPATH + 4];
    if (!ends_with_exe(path)) {
        snprintf(path_exe, sizeof(path_exe), "%s.exe", path);
        exec_path = path_exe;
    }
#endif

    // Check if file exists and get file info
    if (stat(exec_path, &buf) < 0) {
        return -1;  // File doesn't exist
    }

    // Ensure it's a regular file (not directory or device)
    if (!S_ISREG(buf.st_mode)) {
        errno = S_ISDIR(buf.st_mode) ? EISDIR : EPERM;
        return -1;
    }

    // Check read and execute permissions
#ifndef WIN32
    // Unix: Use access() for accurate permission checking
    bool can_read = (access(exec_path, R_OK) == 0);
    bool can_execute = (access(exec_path, X_OK) == 0);
#else
    // Windows: Check permission bits directly
    bool can_read = (buf.st_mode & S_IRUSR) != 0;
    bool can_execute = (buf.st_mode & S_IXUSR) != 0;
    errno = EACCES;  // Set error for potential failure
#endif

    // Return appropriate code based on permissions
    if (!can_execute) return -1;  // Cannot execute
    if (!can_read) return -2;     // Cannot read (needed for dynamic loading)
    return 0;  // Success: file is executable and readable
}
```

Key simplifications made:
- Abstracted Windows .exe handling logic into conceptual flow
- Consolidated permission checking into clearer boolean variables
- Simplified conditional return logic for better readability
- Added descriptive comments for each major validation step
- Focused on the main validation workflow while preserving all essential checks