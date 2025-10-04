# psql_start_test

## Location
[src/test/regress/pg_regress_main.c:29-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress_main.c#L29-L103)

## Overview
Starts a psql test process for a specified test file, handling input/output redirection and setting up the testing environment for PostgreSQL regression tests.

## Definition

```c
static PID_TYPE
psql_start_test(const char *testname,
				_stringlist **resultfiles,
				_stringlist **expectfiles,
				_stringlist **tags)
```
## Detailed Description
This function is a core component of PostgreSQL's regression testing framework. It creates and launches a psql subprocess to execute a specific test case. The function handles file path resolution for input SQL files and expected output files, constructs the appropriate psql command with necessary flags, and manages process spawning. It implements a vpath-like search strategy, looking first in the output directory for local test overrides, then falling back to the input directory. The function sets up environment variables for test identification and ensures proper cleanup of resources.

## Parameters / Member Variables
- `*testname`: The name of the test to run (without .sql extension)
- `**resultfiles`: Pointer to string list that will be populated with result file paths
- `**expectfiles`: Pointer to string list that will be populated with expected output file paths
- `**tags`: Pointer to string list for test tags (currently unused in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [file_exists](../f/file_exists.md): Check if input and expected files exist
  - [add_stringlist_item](../a/add_stringlist_item.md): Add file paths to result and expected file lists
  - [spawn_process](../s/spawn_process.md): Create and start the psql subprocess
  - setenv/unsetenv: Manage PGAPPNAME environment variable
  - [initStringInfo](../i/initStringInfo.md)/appendStringInfo: Build psql command string
  - [pfree](pfree.md): Free allocated memory
- Called from (representative examples):
  - [main](../m/main.md) (in src/test/regress/pg_regress_main.c:115)

## Notes and Other Information
- Returns INVALID_PID on failure and exits the program with code 2
- Uses specific psql flags: -X (no startup file), -a (echo all), -q (quiet), -d (database)
- Sets HIDE_TABLEAM and HIDE_TOAST_COMPRESSION variables to normalize test output across different access methods
- Implements file path fallback strategy for flexibility in test execution environments
- Temporarily sets PGAPPNAME environment variable for process identification during testing

## Simplified Source

```c
static PID_TYPE
psql_start_test(const char *testname,
                _stringlist **resultfiles,
                _stringlist **expectfiles,
                _stringlist **tags)
{
    PID_TYPE pid;
    char infile[MAXPGPATH];
    char outfile[MAXPGPATH];
    char expectfile[MAXPGPATH];
    StringInfoData psql_cmd;
    char *appnameenv;

    // Find input SQL file (output dir first, then input dir)
    snprintf(infile, sizeof(infile), "%s/sql/%s.sql", outputdir, testname);
    if (!file_exists(infile))
        snprintf(infile, sizeof(infile), "%s/sql/%s.sql", inputdir, testname);

    // Set output file path
    snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, testname);

    // Find expected output file (expected dir first, then input dir)
    snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", expecteddir, testname);
    if (!file_exists(expectfile))
        snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", inputdir, testname);

    // Add files to result lists
    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    // Build psql command
    initStringInfo(&psql_cmd);
    if (launcher)
        appendStringInfo(&psql_cmd, "%s ", launcher);

    appendStringInfo(&psql_cmd,
                     "\"%s%spsql\" -X -a -q -d \"%s\" %s < \"%s\" > \"%s\" 2>&1",
                     bindir ? bindir : "",
                     bindir ? "/" : "",
                     dblist->str,
                     "-v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on",
                     infile,
                     outfile);

    // Set application name for test identification
    appnameenv = psprintf("pg_regress/%s", testname);
    setenv("PGAPPNAME", appnameenv, 1);
    free(appnameenv);

    // Start the psql process
    pid = spawn_process(psql_cmd.data);

    if (pid == INVALID_PID) {
        fprintf(stderr, _("could not start process for test %s\n"), testname);
        exit(2);
    }

    // Clean up
    unsetenv("PGAPPNAME");
    pfree(psql_cmd.data);

    return pid;
}
```