# TestSpec

## Location
src/test/isolation/isolationtester.h: 84 - 93

## Overview
A comprehensive structure that defines a complete isolation test specification, containing all setup, teardown, sessions, and permutations needed to execute an isolation test scenario.

## Definition
```c
typedef struct
{
    char      **setupsqls;
    int         nsetupsqls;
    char       *teardownsql;
    Session   **sessions;
    int         nsessions;
    Permutation **permutations;
    int         npermutations;
} TestSpec;
```

## Detailed Description
TestSpec is the top-level data structure in PostgreSQL's isolation testing framework that encapsulates a complete test scenario. It represents the parsed content of an isolation test specification file and contains all the information needed to execute concurrent database operations and verify their behavior. The structure organizes setup operations, multiple database sessions, various execution permutations, and cleanup operations into a single cohesive test definition. This allows the isolation tester to systematically verify database behavior under different concurrent access patterns.

## Parameters / Member Variables
- `setupsqls`: Array of SQL statements to execute before running the test permutations
- `nsetupsqls`: Number of setup SQL statements in the setupsqls array
- `teardownsql`: SQL statement to execute after all test permutations complete (cleanup)
- `sessions`: Array of pointers to Session structures representing concurrent database sessions
- `nsessions`: Number of sessions involved in the test
- `permutations`: Array of pointers to Permutation structures defining different execution orderings
- `npermutations`: Number of permutations to test

## Dependencies
- Functions called/Symbols referenced:
  - Session
  - Permutation
- Called from (representative examples):
  - main
  - check_testspec
  - run_testspec
  - run_all_permutations
  - run_named_permutations
  - run_permutation

## Notes and Other Information
- Central data structure for the isolation testing framework in src/test/isolation/
- Populated by parsing isolation test specification files through spec_yyparse()
- Global instance 'parseresult' holds the parsed test specification
- Used to coordinate complex concurrent testing scenarios across multiple database sessions
- Essential for detecting race conditions, deadlocks, and isolation violations
- Supports both exhaustive permutation testing and named permutation execution
- Integrates setup/teardown operations with concurrent execution testing