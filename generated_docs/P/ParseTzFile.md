# ParseTzFile

## Location
[src/backend/utils/misc/tzparser.c:276-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/tzparser.c#L276-L446)

## Overview
Parses a single timezone abbreviation file with support for recursion to handle @INCLUDE directives and builds an array of timezone entries.

## Definition
static int ParseTzFile(const char *filename, int depth, tzEntry **base, int *arraysize, int n)

## Detailed Description
This function is the core parser for PostgreSQL's timezone abbreviation files. It reads and processes timezone files line by line, supporting special directives like @INCLUDE for file inclusion and @OVERRIDE for duplicate handling. The function enforces security restrictions on filenames, manages recursion depth, and coordinates with helper functions to parse individual lines and maintain the sorted timezone array. It handles file I/O errors gracefully and provides detailed error reporting.

## Parameters / Member Variables
- : User-specified timezone file name (without path, must be alpha characters only)
- : Current recursion depth (limited to 3 levels)
- : Array for results (changeable if array must be enlarged)
- : Allocated length of array (changeable if array must be enlarged)
- : Current number of valid elements in array

## Dependencies
- Functions called/Symbols referenced:
  - isalpha
  - GUC_check_errmsg
  - [get_share_path](../g/get_share_path.md)
  - snprintf
  - [AllocateFile](../A/AllocateFile.md)
  - [AllocateDir](../A/AllocateDir.md)
  - [FreeDir](../F/FreeDir.md)
  - feof
  - fgets
  - ferror
  - strlen
  - isspace
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [pstrdup](../p/pstrdup.md)
  - strtok
  - WHITESPACE
  - [splitTzLine](../s/splitTzLine.md)
  - [validateTzEntry](../v/validateTzEntry.md)
  - [addToArray](../a/addToArray.md)
  - [FreeFile](../F/FreeFile.md)
- Called from (representative examples):
  - [ParseTzFile](ParseTzFile.md) (recursive calls)
  - [load_tzoffsets](../l/load_tzoffsets.md)

## Notes and Other Information
The function enforces several security and sanity checks: filenames must contain only alphabetic characters, recursion is limited to 3 levels, and lines cannot exceed the buffer size. Special directives @INCLUDE and @OVERRIDE provide flexibility in organizing timezone data across multiple files. The parser automatically skips empty lines and comments (lines beginning with #).

## Simplified Source

```c
static int ParseTzFile(const char *filename, int depth,
                      tzEntry **base, int *arraysize, int n) {
    char file_path[MAXPGPATH];
    FILE *tzFile;
    char tzbuf[1024];
    tzEntry tzentry;
    int lineno = 0;
    bool override = false;

    // Validate filename contains only alpha characters
    for (const char *p = filename; *p; p++) {
        if (!isalpha((unsigned char) *p)) {
            if (depth > 0)
                GUC_check_errmsg("invalid time zone file name \"%s\"", filename);
            return -1;
        }
    }

    // Check recursion depth limit
    if (depth > 3) {
        GUC_check_errmsg("time zone file recursion limit exceeded in file \"%s\"",
                        filename);
        return -1;
    }

    // Build file path and open file
    get_share_path(my_exec_path, share_path);
    snprintf(file_path, sizeof(file_path), "%s/timezonesets/%s",
             share_path, filename);
    tzFile = AllocateFile(file_path, "r");
    if (!tzFile) {
        // Error handling for missing file/directory
        return -1;
    }

    // Process each line in the file
    while (!feof(tzFile)) {
        lineno++;
        if (fgets(tzbuf, sizeof(tzbuf), tzFile) == NULL) {
            if (ferror(tzFile)) {
                GUC_check_errmsg("could not read time zone file \"%s\": %m", filename);
                n = -1;
            }
            break;
        }

        // Skip whitespace, empty lines, and comments
        char *line = tzbuf;
        while (*line && isspace((unsigned char) *line)) line++;
        if (*line == '\0' || *line == '#') continue;

        // Handle special directives
        if (pg_strncasecmp(line, "@INCLUDE", strlen("@INCLUDE")) == 0) {
            char *includeFile = pstrdup(line + strlen("@INCLUDE"));
            includeFile = strtok(includeFile, WHITESPACE);
            if (!includeFile || !*includeFile) {
                n = -1;
                break;
            }
            n = ParseTzFile(includeFile, depth + 1, base, arraysize, n);
            if (n < 0) break;
            continue;
        }

        if (pg_strncasecmp(line, "@OVERRIDE", strlen("@OVERRIDE")) == 0) {
            override = true;
            continue;
        }

        // Parse timezone entry line
        if (!splitTzLine(filename, lineno, line, &tzentry) ||
            !validateTzEntry(&tzentry)) {
            n = -1;
            break;
        }

        n = addToArray(base, arraysize, n, &tzentry, override);
        if (n < 0) break;
    }

    FreeFile(tzFile);
    return n;
}
```