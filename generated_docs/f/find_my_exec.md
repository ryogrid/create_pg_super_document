# find_my_exec

## Location
[src/common/exec.c:160-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L160-L240)

## Overview
Finds the absolute path to the current program's executable by searching through the system PATH or validating a direct path, essential for PostgreSQL processes that need to locate themselves and related binaries.

## Definition

```c
int
find_my_exec(const char *argv0, char *retpath)
```
## Detailed Description
The  function determines the absolute path to the currently running executable based on the command-line argument . This is crucial for PostgreSQL because:

1. Dynamic loading on some platforms requires knowing the executable's location
2. An absolute path is needed since the working directory may change later
3. A true path (not a symlink) is required to locate other installation files relative to the executable

The function uses a multi-step approach:
- If  contains a directory separator, it treats it as a direct path and validates it
- On Windows, it also checks the current directory for executables without path separators
- Otherwise, it searches through each directory in the PATH environment variable
- For each potential path, it validates the executable using 
- Upon finding a valid executable, it normalizes the path using 

## Parameters / Member Variables
- `*argv0`: The command-line argument (program name) passed to the program
- `*retpath`: Output buffer where the absolute path will be stored (must be MAXPGPATH size)
## Dependencies
- Functions called/Symbols referenced:
  -  (safe string copying)
  -  (finds directory separators in path)
  -  (validates executable files)
  -  (normalizes and resolves path)
  -  (gets PATH environment variable)
  -  (finds PATH variable separators)
  -  (joins path components)
  -  (canonicalizes path)
  -  (error logging)
- Called from (representative examples):
  -  (postmaster initialization)
  -  (standalone process setup)
  -  (initdb utility)
  -  (finding related executables)
  - Various PostgreSQL utilities and tools

## Notes and Other Information
- Returns 0 on success, -1 on failure
- On Windows, checks current directory first for names without slashes
- Searches PATH in the same order the shell would use
- Handles different types of validation failures appropriately
- Critical for PostgreSQL's ability to find related binaries and shared libraries
- Used extensively throughout PostgreSQL utilities and the main server process

## Simplified Source

```c
// Simplified version of find_my_exec
int find_my_exec(const char *argv0, char *retpath) {
    // Step 1: Copy argv0 to output buffer
    strlcpy(retpath, argv0, MAXPGPATH);

    // Step 2: Check if argv0 contains a path separator (direct path)
    if (first_dir_separator(retpath) != NULL) {
        if (validate_exec(retpath) == 0)
            return normalize_exec_path(retpath);

        // Log error for invalid direct path
        log_error(ERRCODE_WRONG_OBJECT_TYPE, "invalid binary \"%s\"", retpath);
        return -1;
    }

#ifdef WIN32
    // Step 3: On Windows, check current directory first
    if (validate_exec(retpath) == 0)
        return normalize_exec_path(retpath);
#endif

    // Step 4: Search through PATH environment variable
    char *path = getenv("PATH");
    if (path && *path) {
        char *current_dir = path;
        char *next_separator;

        do {
            // Find next directory in PATH
            next_separator = first_path_var_separator(current_dir);
            if (!next_separator)
                next_separator = current_dir + strlen(current_dir);

            // Build full path: directory + "/" + argv0
            strlcpy(retpath, current_dir, Min(next_separator - current_dir + 1, MAXPGPATH));
            join_path_components(retpath, retpath, argv0);
            canonicalize_path(retpath);

            // Try to validate this path
            int validation_result = validate_exec(retpath);
            if (validation_result == 0) {
                // Found valid executable
                return normalize_exec_path(retpath);
            } else if (validation_result == -2) {
                // Found file but couldn't read it
                log_error(ERRCODE_WRONG_OBJECT_TYPE, "could not read binary \"%s\"", retpath);
            }
            // If validation_result == -1, keep searching

            current_dir = next_separator + 1;
        } while (*next_separator);
    }

    // Step 5: Executable not found anywhere
    log_error(ERRCODE_UNDEFINED_FILE, "could not find a \"%s\" to execute", argv0);
    return -1;
}
```

Key simplifications made:
- Removed detailed pointer manipulation in PATH parsing loop
- Simplified variable names (startp/endp → current_dir/next_separator)
- Consolidated switch statement into clearer if-else logic
- Added step-by-step comments for main algorithm phases
- Focused on the core logic flow: direct path → Windows current dir → PATH search
- Abstracted complex error handling details while preserving essential error reporting