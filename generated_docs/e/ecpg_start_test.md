# ecpg_start_test

## Location
[src/interfaces/ecpg/test/pg_regress_ecpg.c:148-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/pg_regress_ecpg.c#L148-L238)

## Overview
Initiates an ECPG test process for a specified test file, setting up input/output file paths, filtering source files, and spawning the test execution with proper redirection.

## Definition
```c
static PID_TYPE ecpg_start_test(const char *testname,
                                _stringlist **resultfiles,
                                _stringlist **expectfiles,
                                _stringlist **tags)
```

## Detailed Description
This function serves as the main test orchestration mechanism for ECPG (Embedded SQL in C) tests. It performs comprehensive setup for test execution including:

1. **File Path Construction**: Creates paths for input programs, source files, and output files (stdout, stderr, source) using standardized naming conventions
2. **Test Name Normalization**: Converts slashes to dashes in test names to create filesystem-safe identifiers
3. **Output Management**: Sets up result and expected file lists with corresponding tags for later comparison
4. **Source Filtering**: Applies source file filtering via ecpg_filter_source to normalize #line directives
5. **Process Execution**: Constructs and executes the test command with proper stdout/stderr redirection
6. **Environment Setup**: Sets PGAPPNAME environment variable for test identification

The function handles the complete lifecycle of test preparation and launch, returning the process ID for monitoring and cleanup by the calling code.

## Parameters / Member Variables
- `testname`: Name of the test to execute (used to locate input files)
- `resultfiles`: Pointer to string list that will contain paths to generated result files
- `expectfiles`: Pointer to string list that will contain paths to expected result files
- `tags`: Pointer to string list that will contain tags ("stdout", "stderr", "source") corresponding to file types

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_filter_source](ecpg_filter_source.md) (source file normalization)
  - [add_stringlist_item](../a/add_stringlist_item.md) (list management)
  - [spawn_process](../s/spawn_process.md) (process creation)
  - setenv/unsetenv (environment variable management)
  - Standard C functions (snprintf, psprintf, free)
- Called from:
  - [main](../m/main.md) (primary test execution loop)
- Data types used:
  - PID_TYPE (process identifier type)
  - [_stringlist](../s/_stringlist.md) (string list structure)
  - [StringInfoData](../S/StringInfoData.md) (PostgreSQL string buffer)

## Notes and Other Information
- This is a static function used internally within the ECPG test framework
- Creates three types of output files: stdout, stderr, and filtered source
- Uses MAXPGPATH for path buffer sizing to handle long filesystem paths
- Implements proper error handling with exit code 2 on process spawn failure
- The PGAPPNAME environment variable helps identify test processes in PostgreSQL logs
- File naming convention converts '/' to '-' to avoid filesystem path conflicts
- Essential component of the PostgreSQL ECPG regression test system
- Located at src/interfaces/ecpg/test/pg_regress_ecpg.c:148-238

## Simplified Source

```c
static PID_TYPE ecpg_start_test(const char *testname,
                                _stringlist **resultfiles,
                                _stringlist **expectfiles,
                                _stringlist **tags) {
    PID_TYPE pid;
    char inprg[MAXPGPATH];
    char insource[MAXPGPATH];
    StringInfoData testname_dash;
    char outfile_stdout[MAXPGPATH], expectfile_stdout[MAXPGPATH];
    char outfile_stderr[MAXPGPATH], expectfile_stderr[MAXPGPATH];
    char outfile_source[MAXPGPATH], expectfile_source[MAXPGPATH];
    char cmd[MAXPGPATH * 3];
    char *appnameenv;

    // Build input file paths
    snprintf(inprg, sizeof(inprg), "%s/%s", inputdir, testname);
    snprintf(insource, sizeof(insource), "%s/%s.c", inputdir, testname);

    // Create filesystem-safe test name (replace '/' with '-')
    initStringInfo(&testname_dash);
    appendStringInfoString(&testname_dash, testname);
    for (char *c = testname_dash.data; *c != '\0'; c++) {
        if (*c == '/')
            *c = '-';
    }

    // Build expected and output file paths
    snprintf(expectfile_stdout, sizeof(expectfile_stdout),
             "%s/expected/%s.stdout", expecteddir, testname_dash.data);
    snprintf(expectfile_stderr, sizeof(expectfile_stderr),
             "%s/expected/%s.stderr", expecteddir, testname_dash.data);
    snprintf(expectfile_source, sizeof(expectfile_source),
             "%s/expected/%s.c", expecteddir, testname_dash.data);

    snprintf(outfile_stdout, sizeof(outfile_stdout),
             "%s/results/%s.stdout", outputdir, testname_dash.data);
    snprintf(outfile_stderr, sizeof(outfile_stderr),
             "%s/results/%s.stderr", outputdir, testname_dash.data);
    snprintf(outfile_source, sizeof(outfile_source),
             "%s/results/%s.c", outputdir, testname_dash.data);

    // Add files to tracking lists
    add_stringlist_item(resultfiles, outfile_stdout);
    add_stringlist_item(expectfiles, expectfile_stdout);
    add_stringlist_item(tags, "stdout");

    add_stringlist_item(resultfiles, outfile_stderr);
    add_stringlist_item(expectfiles, expectfile_stderr);
    add_stringlist_item(tags, "stderr");

    add_stringlist_item(resultfiles, outfile_source);
    add_stringlist_item(expectfiles, expectfile_source);
    add_stringlist_item(tags, "source");

    // Filter source file to normalize #line directives
    ecpg_filter_source(insource, outfile_source);

    // Build command with output redirection
    snprintf(cmd, sizeof(cmd), "\"%s\" >\"%s\" 2>\"%s\"",
             inprg, outfile_stdout, outfile_stderr);

    // Set environment variable for test identification
    appnameenv = psprintf("ecpg/%s", testname_dash.data);
    setenv("PGAPPNAME", appnameenv, 1);
    free(appnameenv);

    // Launch test process
    pid = spawn_process(cmd);

    if (pid == INVALID_PID) {
        fprintf(stderr, "could not start process for test %s\n", testname);
        exit(2);
    }

    // Cleanup
    unsetenv("PGAPPNAME");
    free(testname_dash.data);

    return pid;
}
```