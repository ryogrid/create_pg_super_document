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
- : The name of the isolation test to run (without file extension)
- : Pointer to string list where the output file path will be added
- : Pointer to string list where the expected results file path will be added  
- : Pointer to string list for test tags (currently unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - find_other_exec
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