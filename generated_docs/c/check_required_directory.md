# check_required_directory

## Location
src/bin/pg_upgrade/option.c: 359 - 403

## Overview
Validates and resolves directory paths for pg_upgrade, checking command-line arguments, environment variables, or current working directory as fallbacks.

## Definition
```c
static void check_required_directory(char **dirpath, const char *envVarName, bool useCwd,
                                   const char *cmdLineOption, const char *description,
                                   bool missingOk)
```

## Detailed Description
This function validates directory options for pg_upgrade with a flexible resolution hierarchy. It first checks if a directory path was provided via command line, then falls back to environment variables, and optionally to the current working directory. The function handles missing directories gracefully when allowed, performs path canonicalization to clean up trailing separators, and provides meaningful error messages with specific option and environment variable names when validation fails.

## Parameters / Member Variables
- `dirpath`: Pointer to directory path string (input/output parameter, modified if resolved from env/cwd)
- `envVarName`: Name of environment variable to check if dirpath is null/empty
- `useCwd`: Whether to use current working directory as fallback
- `cmdLineOption`: Command-line option name for error messages (e.g., "-d", "-D")
- `description`: Human-readable description of directory purpose for error messages
- `missingOk`: Whether it's acceptable for both dirpath and environment variable to be missing

## Dependencies
- Functions called/Symbols referenced:
  - getenv (to check environment variables)
  - strlen (for string length validation)
  - pg_strdup (for string duplication)
  - getcwd (to get current working directory)
  - pg_fatal (for error reporting)
  - canonicalize_path (for path cleanup)
- Called from (representative examples):
  - parseCommandLine (src/bin/pg_upgrade/option.c:246-254) - called 5 times for different directories

## Notes and Other Information
- Static function only accessible within option.c
- Used to validate old/new data directories, binary directories, and socket directory
- Path canonicalization removes trailing separators to ensure consistent path construction
- Error messages are user-friendly and include specific option names and environment variables
- Supports optional directories (missingOk=true) for non-critical paths like socket directories
- Follows priority: command line → environment variable → current directory (if useCwd=true) → error/optional