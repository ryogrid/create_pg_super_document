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