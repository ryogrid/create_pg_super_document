# run_single_test

## Location
[src/test/regress/pg_regress.c:1844-1910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1844-L1910)

## Overview
Executes a single PostgreSQL regression test and compares the results with expected output to determine test success or failure.

## Definition

```c
static void
run_single_test(const char *test, test_start_function startfunc,
				postprocess_result_function postfunc)
```
## Detailed Description
This function orchestrates the execution of a single test in the PostgreSQL regression test suite. It starts the test using the provided start function, waits for completion, and then compares the actual results with expected results. The function handles timing measurements, result file comparisons, and status reporting.

The function works by:
1. Launching the test using the provided start function
2. Measuring execution time from start to completion
3. Waiting for the test process to complete
4. Comparing result files with expected files line by line
5. Reporting test status (pass/fail) with timing information
6. Handling optional post-processing of result files

## Parameters / Member Variables
- `*test`: Name of the test to execute
- `startfunc`: Function pointer to start the test process, returns PID and populates file lists
- `postfunc`: Optional function pointer for post-processing result files
## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_SET_CURRENT (timing measurement)
  - [wait_for_tests](../w/wait_for_tests.md) (wait for test completion) 
  - [results_differ](results_differ.md) (compare result vs expected files)
  - [test_status_failed](../t/test_status_failed.md) (report test failure)
  - [test_status_ok](../t/test_status_ok.md) (report test success)
  - [log_child_failure](../l/log_child_failure.md) (log process exit failures)
  - diag (diagnostic output)
  - INSTR_TIME_SUBTRACT (timing calculations)
  - INSTR_TIME_GET_MILLISEC (timing conversion)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- This is a static function used internally by the pg_regress test framework
- Supports optional tagging of test results for better diagnostics
- Handles multiple result/expected file pairs per test
- Times test execution in milliseconds for performance tracking
- Distinguishes between process failures and result mismatches
- Part of PostgreSQL's comprehensive regression test infrastructure

## Simplified Source

```c
static void run_single_test(const char *test, test_start_function startfunc,
                            postprocess_result_function postfunc) {
    PID_TYPE pid;
    instr_time starttime, stoptime;
    int exit_status;
    _stringlist *resultfiles = NULL;
    _stringlist *expectfiles = NULL;
    _stringlist *tags = NULL;
    bool differ = false;

    // Start test and record start time
    pid = startfunc(test, &resultfiles, &expectfiles, &tags);
    INSTR_TIME_SET_CURRENT(starttime);

    // Wait for test completion and record end time
    wait_for_tests(&pid, &exit_status, &stoptime, NULL, 1);

    // Compare result files with expected files
    for (_stringlist *rl = resultfiles, *el = expectfiles, *tl = tags;
         rl != NULL;
         rl = rl->next, el = el->next, tl = tl ? tl->next : NULL) {

        // Optional post-processing
        if (postfunc)
            postfunc(rl->str);

        // Check for differences
        bool newdiff = results_differ(test, rl->str, el->str);
        if (newdiff && tl)
            diag("tag: %s", tl->str);
        differ |= newdiff;
    }

    // Calculate elapsed time
    INSTR_TIME_SUBTRACT(stoptime, starttime);
    double elapsed = INSTR_TIME_GET_MILLISEC(stoptime);

    // Report test results
    if (exit_status != 0) {
        test_status_failed(test, elapsed, false);
        log_child_failure(exit_status);
    } else if (differ) {
        test_status_failed(test, elapsed, false);
    } else {
        test_status_ok(test, elapsed, false);
    }
}
```