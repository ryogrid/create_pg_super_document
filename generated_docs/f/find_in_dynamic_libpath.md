# find_in_dynamic_libpath

## Location
[src/backend/utils/fmgr/dfmgr.c:515-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L515-L598)

## Overview
Searches for a library file by basename in the configured dynamic library search path and returns the full path if found.

## Definition

```c
static char *
find_in_dynamic_libpath(const char *basename)
```
## Detailed Description
This function implements a comprehensive search mechanism for dynamic libraries within PostgreSQL's configured library search paths. It processes the  configuration parameter, which contains a colon-separated list of directory paths where libraries should be searched.

The search process involves:
1. Validating that the basename contains no directory separators (must be a simple filename)
2. Parsing the  string by splitting on path separators
3. For each path component:
   - Extracting the individual directory path
   - Applying macro substitution using 
   - Canonicalizing the path to resolve any relative components
   - Validating that the resulting path is absolute
   - Constructing the full file path by combining directory and basename
   - Testing for file existence using 
4. Returning the first matching full path found, or NULL if no file is located

The function includes robust error checking, reporting errors for zero-length path components and non-absolute paths in the search configuration.

## Parameters / Member Variables
- `*basename`: The simple filename (without directory components) to search for in the library path
## Dependencies
- Functions called/Symbols referenced:
  -  - validates that basename contains no directory separators
  -  - locates path separators in the search path string
  -  - safely copies string segments
  -  - expands macros like $libdir in path components
  -  - resolves relative path components
  -  - validates that paths are absolute
  -  - tests for file existence
  - / - PostgreSQL memory management
  -  - constructs full file paths
  -  - [debug](../d/debug.md) logging
  -  - global configuration variable
- Called from:
  -  (src/backend/utils/fmgr/dfmgr.c:426, 442)

## Notes and Other Information
- The function is static, limiting its scope to the dfmgr.c compilation unit
- Requires that the basename parameter contains no directory separators (enforced by assertion)
- Uses DEBUG3 logging level to trace search attempts for debugging purposes
- Implements thorough validation including zero-length component detection and absolute path requirements
- Part of PostgreSQL's dynamic library loading infrastructure
- Returns freshly palloc'd memory that must be freed by the caller
- The search stops at the first matching file found, implementing a priority-based search order
- Critical for PostgreSQL's extension loading mechanism and external function libraries

## Simplified Source

```c
// Simplified version of find_in_dynamic_libpath
static char *
find_in_dynamic_libpath(const char *basename)
{
    const char *search_path;
    size_t basename_len;

    // Validate inputs and get search path
    Assert(basename != NULL);
    Assert(first_dir_separator(basename) == NULL);  // basename only, no paths

    search_path = Dynamic_library_path;
    if (strlen(search_path) == 0)
        return NULL;

    basename_len = strlen(basename);

    // Search through each directory in the colon-separated path
    for (;;) {
        char *path_component;
        char *expanded_path;
        char *full_filepath;
        size_t component_len;

        // Extract next path component from search_path
        char *separator = first_path_var_separator(search_path);

        if (separator == search_path)
            ereport(ERROR, "zero-length component in dynamic_library_path");

        // Calculate component length and extract it
        component_len = (separator == NULL) ? strlen(search_path) : (separator - search_path);
        path_component = palloc(component_len + 1);
        strlcpy(path_component, search_path, component_len + 1);

        // Expand macros (like $libdir) and canonicalize path
        expanded_path = substitute_libpath_macro(path_component);
        pfree(path_component);
        canonicalize_path(expanded_path);

        // Ensure absolute path
        if (!is_absolute_path(expanded_path))
            ereport(ERROR, "component in dynamic_library_path is not absolute");

        // Construct full file path: directory + "/" + basename
        full_filepath = palloc(strlen(expanded_path) + 1 + basename_len + 1);
        sprintf(full_filepath, "%s/%s", expanded_path, basename);
        pfree(expanded_path);

        // Check if file exists at this path
        if (pg_file_exists(full_filepath))
            return full_filepath;  // Found it! Return full path

        pfree(full_filepath);

        // Move to next path component or exit if done
        if (search_path[component_len] == '\0')
            break;
        else
            search_path += component_len + 1;
    }

    return NULL;  // File not found in any search path
}
```

Key simplifications made:
- Renamed variables for clarity (`p` → `search_path`, `piece` → `path_component`, etc.)
- Added explanatory comments for each major step
- Grouped related operations together logically
- Removed DEBUG3 logging call for focus on core logic
- Simplified loop structure while preserving exact behavior
- Made memory management operations more explicit with descriptive comments
- Consolidated error handling patterns