# ecpg_filter_source

## Location
[src/interfaces/ecpg/test/pg_regress_ecpg.c:34-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/pg_regress_ecpg.c#L34-L92)

## Overview
Filters source files by normalizing #line directives, removing path components to make output consistent across different build environments and platforms.

## Definition

```c
static void
ecpg_filter_source(const char *sourcefile, const char *outfile)
```
## Detailed Description
This function creates a filtered copy of a source file, specifically designed to normalize #line preprocessor directives. It removes path components from file references in #line directives to ensure consistent output regardless of compiler, platform, or build configuration differences. For example, it transforms  into  by stripping the relative path portion.

The function processes the input file line by line, detecting lines that start with "#line " and then removing any leading path components (sequences of '.' and '/') from the quoted filename portion of the directive. This normalization is crucial for regression testing where build paths may vary between environments.

## Parameters / Member Variables
- `*sourcefile`: Input source file path to be filtered
- `*outfile`: Output file path where the filtered content will be written
## Dependencies
- Functions called/Symbols referenced:
  - fopen (for file I/O operations)
  - [pg_get_line_buf](../p/pg_get_line_buf.md) (PostgreSQL utility for line reading)
  - [initStringInfo](../i/initStringInfo.md), pfree (PostgreSQL string utilities)
  - Standard C string functions (strstr, strchr, memmove, strlen)
- Called from:
  - [ecpg_start_test](ecpg_start_test.md) (main test execution function)

## Notes and Other Information
- This is a static function used internally within the ECPG test framework
- Essential for consistent regression test output across different build environments
- Handles memory management properly using PostgreSQL's StringInfo utilities
- Exits with error code 2 if file operations fail
- Part of the PostgreSQL ECPG (Embedded SQL in C) testing infrastructure located at src/interfaces/ecpg/test/pg_regress_ecpg.c:34-92

## Simplified Source

```c
static void ecpg_filter_source(const char *sourcefile, const char *outfile) {
    FILE *s, *t;
    StringInfoData linebuf;

    // Open input and output files
    s = fopen(sourcefile, "r");
    if (!s) {
        fprintf(stderr, "Could not open file %s for reading\n", sourcefile);
        exit(2);
    }
    t = fopen(outfile, "w");
    if (!t) {
        fprintf(stderr, "Could not open file %s for writing\n", outfile);
        exit(2);
    }

    initStringInfo(&linebuf);

    // Process each line from input file
    while (pg_get_line_buf(s, &linebuf)) {
        // Check if line starts with "#line "
        if (strstr(linebuf.data, "#line ") == linebuf.data) {
            char *p = strchr(linebuf.data, '"');
            int plen = 1;

            // Skip over path components (. and /)
            while (*p && (*(p + plen) == '.' || strchr(p + plen, '/') != NULL)) {
                plen++;
            }

            // Remove path prefix if found
            if (plen > 1) {
                memmove(p + 1, p + plen, strlen(p + plen) + 1);
            }
        }

        // Write the processed line to output
        fputs(linebuf.data, t);
    }

    // Cleanup
    pfree(linebuf.data);
    fclose(s);
    fclose(t);
}
```