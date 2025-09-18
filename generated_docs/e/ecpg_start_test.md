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
  - _stringlist (string list structure)
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