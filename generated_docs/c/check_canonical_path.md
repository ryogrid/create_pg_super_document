# check_canonical_path

## Location
[src/backend/commands/variable.c:1047-1067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1047-L1067)

## Overview
The `check_canonical_path` function validates and normalizes file system path values for various PostgreSQL configuration parameters.

## Definition
```c
bool check_canonical_path(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function is a GUC (Grand Unified Configuration) check hook designed specifically for path-related configuration variables such as log_directory and external_pid_file. Its primary purpose is to canonicalize (normalize) file system paths to ensure consistent representation across different platforms and input formats.

The canonicalization process performed by this function includes:
- Resolving relative path components (. and ..)
- Normalizing path separators to the platform standard
- Removing redundant separators
- Ensuring consistent path representation

The function operates in-place on the provided string, taking advantage of the fact that canonicalize_path never enlarges the string, only normalizes it. This approach is memory-efficient and avoids unnecessary allocations.

The function is particularly important for configuration parameters that represent file system paths, ensuring that different ways of specifying the same path (e.g., "/var/log/postgres" vs "/var/log/../log/postgres") result in the same canonical representation.

## Parameters / Member Variables
- `newval`: Pointer to the path string being validated; modified in-place during canonicalization
- `extra`: Output parameter for additional data (unused in this function, set to maintain GUC hook interface)
- `source`: The source of the configuration change (unused in this function but required for GUC hook interface)

## Dependencies
- Functions called/Symbols referenced:
  - [canonicalize_path](canonicalize_path.md)
  - GucSource (type definition)
- Called from (representative examples):
  - GUC system framework (as check hook for path parameters)

## Notes and Other Information
- Handles NULL values gracefully (important for optional path parameters like external_pid_file)
- Always returns true, indicating that path canonicalization cannot fail
- Operates in-place to avoid memory allocation overhead
- Used for multiple path-related GUC variables throughout PostgreSQL
- Part of PostgreSQL's configuration validation infrastructure
- Ensures cross-platform path consistency
- Critical for proper file system operations in various PostgreSQL components

## Simplified Source

```c
bool check_canonical_path(char **newval, void **extra, GucSource source)
{
    // Canonicalize the path in-place if not NULL
    if (*newval)
        canonicalize_path(*newval);

    return true;  // Path canonicalization never fails
}
```