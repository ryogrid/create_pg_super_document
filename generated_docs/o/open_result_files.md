# open_result_files

## Location
[src/test/regress/pg_regress.c:1911-1943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1911-L1943)

## Overview
Initializes the output directory structure and creates summary output files for PostgreSQL regression testing.

## Definition

```c
static void
open_result_files(void)
```
## Detailed Description
This function sets up the file system infrastructure needed for regression test output. It creates the main output directory if it doesn't exist, initializes key output files (regression log and diffs), and ensures the results subdirectory is available. The function prepares the environment for capturing test execution logs and storing test result comparisons.

The function performs the following operations:
1. Creates the main output directory if it doesn't exist
2. Opens regression.out as the main log file for continuous writing
3. Creates regression.diffs as an empty file for storing test differences
4. Creates the results subdirectory for individual test output files

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [directory_exists](../d/directory_exists.md) (check if directory exists)
  - [make_directory](../m/make_directory.md) (create directory)
  - fopen (open files for writing)
  - bail (error handling and exit)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - snprintf (formatted string creation)
  - fclose (close file handle)
- Called from (representative examples):
  - [regression_main](../r/regression_main.md) (main regression test entry point)

## Notes and Other Information
- This is a static function used internally by the pg_regress framework
- Creates global file handles (logfile) that remain open for the duration of testing
- The diffs file is created empty and closed immediately - it's opened/closed as needed during testing
- Uses MAXPGPATH constant for path buffer sizing
- Error handling uses the bail() function which terminates the program on failure
- Part of the test setup phase that occurs before any individual tests are run

## Simplified Source

```c
static void open_result_files(void) {
    char file[MAXPGPATH];
    FILE *difffile;

    // Create main output directory if needed
    if (!directory_exists(outputdir))
        make_directory(outputdir);

    // Create and open the main regression log file
    snprintf(file, sizeof(file), "%s/regression.out", outputdir);
    logfilename = pg_strdup(file);
    logfile = fopen(logfilename, "w");
    if (!logfile)
        bail("could not open file \"%s\" for writing: %m", logfilename);

    // Create empty diffs file for test comparisons
    snprintf(file, sizeof(file), "%s/regression.diffs", outputdir);
    difffilename = pg_strdup(file);
    difffile = fopen(difffilename, "w");
    if (!difffile)
        bail("could not open file \"%s\" for writing: %m", difffilename);

    // Close diffs file - it will be reopened as needed
    fclose(difffile);

    // Create results subdirectory for individual test outputs
    snprintf(file, sizeof(file), "%s/results", outputdir);
    if (!directory_exists(file))
        make_directory(file);
}
```