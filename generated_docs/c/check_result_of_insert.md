# check_result_of_insert

## Location
[src/interfaces/ecpg/test/expected/sql-prepareas.c:29-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/sql-prepareas.c#L29-L52)

## Overview
A static utility function in the ECPG test suite that verifies the result of database insert operations by executing a SELECT query and displaying the retrieved values.

## Definition

```c
static void
check_result_of_insert(void)
```
## Detailed Description
This function is part of the ECPG (Embedded SQL in C) test suite, specifically designed to validate that insert operations have been executed correctly. It performs a SELECT query on a test table to retrieve two integer columns (c1 and c2) and displays their values to stdout. The function uses ECPG's embedded SQL functionality with proper error handling through the sqlca (SQL Communication Area) structure.

The function demonstrates typical ECPG usage patterns including:
- Variable declaration sections with ECPG pragmas
- Embedded SQL execution using ECPGdo()
- Error checking via sqlca.sqlcode
- Result display for verification purposes

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGdo](../E/ECPGdo.md) (ECPG runtime function for SQL execution)
  - printf (standard C library function)
  - [sqlprint](../s/sqlprint.md) (ECPG error printing function)
  
- Called from (representative examples):
  - [main](../m/main.md) (multiple times throughout the test program at various line numbers: 128, 144, 174, 201, 228, 255, 282, 309, 362, 399, 436, 473, 510, 541, 572, 607, 642)

## Notes and Other Information
- This is a test-specific function located in the ECPG test expected output file
- Uses ECPG's embedded SQL syntax with #line directives for source mapping
- Implements standard ECPG error handling pattern with sqlca.sqlcode checking
- The function is static, meaning it's only accessible within the same compilation unit
- Part of the prepareas test case which tests prepared statement functionality in ECPG
- The SELECT query retrieves exactly two integer values which are immediately printed for verification