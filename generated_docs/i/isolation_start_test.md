# isolation_start_test

## Location
[src/test/isolation/isolation_main.c:29-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolation_main.c#L29-L110)

## Overview
Starts an isolation tester process for a specified test file and returns the process ID.

## Definition

```c
static PID_TYPE
isolation_start_test(const char *testname,
					 _stringlist **resultfiles,
					 _stringlist **expectfiles,
					 _stringlist **tags)
```
## Detailed Description
This function is responsible for launching an isolation test by executing the  binary with the appropriate input and output files. It performs path lookups for test specification files, manages file paths for input, output, and expected result files, constructs the command line for the isolationtester process, and spawns the process. The function handles file location logic that searches in both output and input directories, following a vpath-like search pattern for flexibility in test execution environments.

## Parameters / Member Variables
- `*testname`: The name of the isolation test to run (without file extension)
- `**resultfiles`: Pointer to string list where the output file path will be added
- `**expectfiles`: Pointer to string list where the expected results file path will be added
- `**tags`: Pointer to string list for test tags (currently unused in function body)
## Dependencies
- Functions called/Symbols referenced:
  - [find_other_exec](../f/find_other_exec.md)
  - [file_exists](../f/file_exists.md)
  - [add_stringlist_item](../a/add_stringlist_item.md)
  - [spawn_process](../s/spawn_process.md)
  - setenv/unsetenv
  - snprintf
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Performs lazy lookup of the isolationtester binary using 
- Sets PGAPPNAME environment variable during test execution for process identification
- Handles file path resolution by checking output directory first, then input directory
- Uses command line redirection to capture test output
- Returns INVALID_PID and exits on failure to start the process
- Part of PostgreSQL's isolation testing framework for concurrent transaction testing

## Simplified Source

```c
static PID_TYPE isolation_start_test(const char *testname,
                                   _stringlist **resultfiles,
                                   _stringlist **expectfiles,
                                   _stringlist **tags) {
    // Locate isolationtester binary on first use
    if (!looked_up_isolation_exec) {
        if (find_other_exec(saved_argv0, "isolationtester", PG_ISOLATION_VERSIONSTR, isolation_exec) != 0) {
            fprintf(stderr, "could not find proper isolationtester binary\n");
            exit(2);
        }
        looked_up_isolation_exec = true;
    }

    // Build file paths for input (.spec), output (.out), and expected results
    char infile[MAXPGPATH], outfile[MAXPGPATH], expectfile[MAXPGPATH];

    // Try output dir first, then input dir (vpath search)
    snprintf(infile, sizeof(infile), "%s/specs/%s.spec", outputdir, testname);
    if (!file_exists(infile))
        snprintf(infile, sizeof(infile), "%s/specs/%s.spec", inputdir, testname);

    snprintf(outfile, sizeof(outfile), "%s/results/%s.out", outputdir, testname);
    snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", outputdir, testname);
    if (!file_exists(expectfile))
        snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out", inputdir, testname);

    // Add files to result lists
    add_stringlist_item(resultfiles, outfile);
    add_stringlist_item(expectfiles, expectfile);

    // Build command: isolationtester "dbname=..." < input.spec > output.out 2>&1
    StringInfoData cmd;
    initStringInfo(&cmd);
    if (launcher) appendStringInfo(&cmd, "%s ", launcher);
    appendStringInfo(&cmd, "\"%s\" \"dbname=%s\" < \"%s\" > \"%s\" 2>&1",
                     isolation_exec, dblist->str, infile, outfile);

    // Set app name and spawn process
    char *appname = psprintf("isolation/%s", testname);
    setenv("PGAPPNAME", appname, 1);

    PID_TYPE pid = spawn_process(cmd.data);
    if (pid == INVALID_PID) {
        fprintf(stderr, "could not start process for test %s\n", testname);
        exit(2);
    }

    // Cleanup
    unsetenv("PGAPPNAME");
    free(appname);
    pfree(cmd.data);

    return pid;
}
```