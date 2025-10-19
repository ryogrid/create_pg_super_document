# load_resultmap

## Location
[src/test/regress/pg_regress.c:615-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L615-L688)

## Overview
Scans the resultmap file to determine which platform-specific expected files to use during regression testing.

## Definition

```c
static void
load_resultmap(void)
```
## Detailed Description
The  function reads a "resultmap" file from the input directory to identify platform-specific expected output files for PostgreSQL regression tests. The resultmap file format uses entries like , where the hostplatformpattern is evaluated as a regular expression against the current platform's config.guess output. When a pattern matches the current host platform, the corresponding test name, file type, and expected result file are stored in a linked list for later use during test execution.

The function implements a last-match-wins strategy by prepending new entries to the front of the resultmap list, ensuring that later entries in the file take precedence over earlier ones in cases of ambiguous matches.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - fopen
  - bail
  - [string_matches_pattern](../s/string_matches_pattern.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [_resultmap](../r/_resultmap.md) (struct type)
- Called from (representative examples):
  - [initialize_environment](../i/initialize_environment.md)

## Notes and Other Information
- The resultmap file is optional - the function silently returns if the file doesn't exist
- Uses a simplified regular expression matching via  rather than full regex support
- Builds a linked list of platform-specific result mappings stored in the global  variable
- Part of the PostgreSQL regression testing framework (pg_regress)
- The file format parsing is strict and will bail out on malformed entries

## Simplified Source

```c
static void load_resultmap(void) {
    char buf[MAXPGPATH];
    FILE *f;

    // Try to open the resultmap file from input directory
    snprintf(buf, sizeof(buf), "%s/resultmap", inputdir);
    f = fopen(buf, "r");
    if (!f) {
        if (errno == ENOENT)
            return;  // File doesn't exist - that's OK
        bail("could not open file \"%s\" for reading: %m", buf);
    }

    // Process each line in the resultmap file
    while (fgets(buf, sizeof(buf), f)) {
        char *platform, *file_type, *expected;

        // Strip trailing whitespace
        int i = strlen(buf);
        while (i > 0 && isspace((unsigned char) buf[i - 1]))
            buf[--i] = '\0';

        // Parse line format: testname:file_type:platform=expected_file
        file_type = strchr(buf, ':');
        if (!file_type)
            bail("incorrectly formatted resultmap entry: %s", buf);
        *file_type++ = '\0';

        platform = strchr(file_type, ':');
        if (!platform)
            bail("incorrectly formatted resultmap entry: %s", buf);
        *platform++ = '\0';

        expected = strchr(platform, '=');
        if (!expected)
            bail("incorrectly formatted resultmap entry: %s", buf);
        *expected++ = '\0';

        // If platform pattern matches current host, add to resultmap list
        if (string_matches_pattern(host_platform, platform)) {
            _resultmap *entry = pg_malloc(sizeof(_resultmap));
            entry->test = pg_strdup(buf);
            entry->type = pg_strdup(file_type);
            entry->resultfile = pg_strdup(expected);
            entry->next = resultmap;
            resultmap = entry;  // Add to front (last match wins)
        }
    }
    fclose(f);
}
```