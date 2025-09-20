# log_child_failure

## Location
[src/test/regress/pg_regress.c:1615-1637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1615-L1637)

## Overview
Reports detailed diagnostic information about failed test processes, including exit codes, termination signals, and human-readable error descriptions.

## Definition

```c
static void
log_child_failure(int exitstatus)
```
## Detailed Description
This function provides comprehensive error reporting for failed test processes in PostgreSQL's regression testing framework. It analyzes the exit status from a child process and generates appropriate diagnostic messages based on how the process terminated. The function distinguishes between normal exits with non-zero status codes, signal-based terminations, and unrecognized failure modes. On Unix/Linux systems, it provides human-readable signal names and descriptions, while on Windows it reports exception codes. This detailed reporting helps developers understand why tests failed and aids in debugging test infrastructure issues.

## Parameters / Member Variables
- : The exit status returned by a failed test process (typically from wait() or similar)

## Dependencies
- Functions called/Symbols referenced:
  - WIFEXITED, WEXITSTATUS (process exit status checking)
  - WIFSIGNALED, WTERMSIG (signal termination checking)
  - diag (diagnostic message output)
  - pg_strsignal (signal name/description lookup on Unix/Linux)
- Called from (representative examples):
  - Used in MAX_PARALLEL_TESTS context (src/test/regress/pg_regress.c:1812)
  - [run_single_test](../r/run_single_test.md) (src/test/regress/pg_regress.c:1892)

## Notes and Other Information
- Handles three types of process termination: normal exit with non-zero code, signal termination, and unrecognized status
- Provides platform-specific reporting (signal names on Unix/Linux, exception codes on Windows)
- Uses the diag() function for consistent diagnostic output formatting
- Essential for debugging test failures and infrastructure problems
- Helps distinguish between test logic failures and system-level issues (crashes, signals)
- Part of PostgreSQL's comprehensive test failure reporting system
- Only called for non-zero exit statuses (successful tests don't trigger this function)