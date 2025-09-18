# ecpg_postprocess_result

## Location
src/interfaces/ecpg/test/pg_regress_ecpg.c: 239 - 253

## Overview
Post-processes ECPG test result files by applying appropriate filtering based on file type, currently focusing on stderr file normalization.

## Definition
```c
static void ecpg_postprocess_result(const char *filename)
```

## Detailed Description
This function serves as a dispatcher for post-processing operations on ECPG test result files. Currently, it specifically handles stderr files by detecting them based on their ".stderr" file extension and applying connection error message filtering through ecpg_filter_stderr. The function is designed to be extensible for future post-processing needs on other file types.

The function examines the filename extension and applies type-specific filtering. For stderr files, it creates a temporary file and invokes the stderr filtering process to normalize connection failure messages, ensuring consistent test results across different environments.

## Parameters / Member Variables
- `filename`: Path to the result file that needs post-processing

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_filter_stderr](ecpg_filter_stderr.md) (stderr file filtering and normalization)
  - [psprintf](../p/psprintf.md) (PostgreSQL string formatting utility)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - Standard C functions (strlen, strcmp)
- Called from:
  - [main](../m/main.md) (test execution framework)

## Notes and Other Information
- This is a static function used internally within the ECPG test framework
- Currently only processes stderr files but designed for future extensibility to other file types
- Uses file extension detection (".stderr") to determine processing type
- Creates temporary files with ".tmp" extension during filtering operations
- Part of the test result normalization pipeline to ensure consistent regression test output
- The comment indicates that only stderr files require filtering "at the moment", suggesting planned expansion
- Located at src/interfaces/ecpg/test/pg_regress_ecpg.c:239-253