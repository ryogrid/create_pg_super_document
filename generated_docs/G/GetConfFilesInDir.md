# GetConfFilesInDir

## Location
[src/backend/utils/misc/conffiles.c:70-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/conffiles.c#L70-L164)

## Overview
Returns an alphabetically sorted list of configuration files (ending in ".conf") found in a specified directory, with comprehensive error handling and validation.

## Definition
```c
char **GetConfFilesInDir(const char *includedir, const char *calling_file, 
                        int elevel, int *num_filenames, char **err_msg)
```

## Detailed Description
This function scans a directory for configuration files and returns them as an array of absolute file paths. It implements several important behaviors:
- Only includes files with ".conf" extension and at least 6 characters in length
- Explicitly excludes hidden files (starting with ".") and system entries like "." and ".."
- Converts the directory path to absolute using AbsoluteConfigLocation
- Sorts results alphabetically for consistent processing order
- Provides detailed error reporting for various failure conditions
- Uses dynamic memory allocation, growing the array in blocks of 32 entries

The function validates the input directory name to prevent empty or blank-only names that could cause recursive inclusion issues.

## Parameters / Member Variables
- `includedir`: The directory path to scan for configuration files
- `calling_file`: The file that requested this directory scan (used for relative path resolution)
- `elevel`: Error reporting level for ereport() calls
- `num_filenames`: Output parameter returning the number of files found
- `err_msg`: Output parameter for detailed error message on failure

## Dependencies
- Functions called/Symbols referenced:
  - [AbsoluteConfigLocation](../A/AbsoluteConfigLocation.md)
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - [join_path_components](../j/join_path_components.md)
  - [canonicalize_path](../c/canonicalize_path.md)
  - [get_dirent_type](../g/get_dirent_type.md)
  - qsort
  - [pg_qsort_strcmp](../p/pg_qsort_strcmp.md)
  - [palloc](../p/palloc.md)/repalloc/pstrdup
  - ereport/errcode/errmsg
- Called from (representative examples):
  - [tokenize_auth_file](../t/tokenize_auth_file.md) (hba.c)

## Notes and Other Information
- Returns NULL on error with details in err_msg parameter
- Caller is responsible for freeing the returned array and all contained strings
- Uses PostgreSQL's memory allocation functions (palloc/repalloc) for automatic cleanup
- Part of PostgreSQL's configuration file inclusion system, particularly for processing include_dir directives
- Implements robust error handling with both ereport() logging and caller-visible error messages
- The 6-character minimum length requirement effectively enforces the ".conf" extension while allowing for at least one character in the base filename

## Simplified Source

```c
// Simplified version of GetConfFilesInDir
char **GetConfFilesInDir(const char *includedir, const char *calling_file,
                        int elevel, int *num_filenames, char **err_msg) {
    char *directory;
    DIR *d;
    struct dirent *de;
    char **filenames = NULL;
    int size_filenames;

    // Validate directory name - reject empty or blank-only names
    if (strspn(includedir, " \t\r\n") == strlen(includedir)) {
        *err_msg = "empty configuration directory name";
        return NULL;
    }

    // Convert to absolute path and open directory
    directory = AbsoluteConfigLocation(includedir, calling_file);
    d = AllocateDir(directory);
    if (d == NULL) {
        *err_msg = psprintf("could not open directory \"%s\"", directory);
        goto cleanup;
    }

    // Initialize dynamic array for filenames
    size_filenames = 32;
    filenames = (char **) palloc(size_filenames * sizeof(char *));
    *num_filenames = 0;

    // Scan directory for .conf files
    while ((de = ReadDir(d, directory)) != NULL) {
        char filename[MAXPGPATH];

        // Filter: must be .conf files, not hidden, minimum 6 chars
        if (strlen(de->d_name) < 6 ||
            de->d_name[0] == '.' ||
            strcmp(de->d_name + strlen(de->d_name) - 5, ".conf") != 0) {
            continue;
        }

        // Build full path and check if it's a regular file
        join_path_components(filename, directory, de->d_name);
        canonicalize_path(filename);

        if (get_dirent_type(filename, de, true, elevel) == PGFILETYPE_ERROR) {
            *err_msg = psprintf("could not stat file \"%s\"", filename);
            pfree(filenames);
            filenames = NULL;
            goto cleanup;
        }

        // Add to array, expanding if necessary
        if (*num_filenames >= size_filenames) {
            size_filenames += 32;
            filenames = (char **) repalloc(filenames, size_filenames * sizeof(char *));
        }
        filenames[*num_filenames] = pstrdup(filename);
        (*num_filenames)++;
    }

    // Sort filenames alphabetically
    if (*num_filenames > 0) {
        qsort(filenames, *num_filenames, sizeof(char *), pg_qsort_strcmp);
    }

cleanup:
    if (d) FreeDir(d);
    pfree(directory);
    return filenames;
}
```

Key simplifications made:
- Consolidated error handling logic while preserving essential checks
- Simplified the file filtering logic into a single if statement
- Removed detailed ereport calls, keeping only the essential error message setting
- Combined directory entry type checking with the main filtering logic
- Maintained the core algorithm: validate input, scan directory, filter files, sort results
- Preserved memory management and cleanup patterns