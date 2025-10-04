# execute_test

## Location
[src/interfaces/ecpg/test/expected/sql-declare.c:216-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/sql-declare.c#L216-L580)

## Overview
The execute_test function serves as a comprehensive test suite for PostgreSQL's ECPG (Embedded SQL in C) functionality, specifically testing DECLARE STATEMENT operations across different database connections and execution methods.

## Definition

```c
*/
void execute_test(void)
```
## Detailed Description
The execute_test function implements a comprehensive test suite for ECPG's DECLARE STATEMENT functionality. It executes four distinct test cases that validate different scenarios of prepared statement usage across multiple database connections (con1, con2, and default connection). The function tests various combinations of DECLARE STATEMENT, PREPARE, CURSOR, and EXECUTE operations, ensuring proper functionality with and without explicit connection specifications (AT clauses).

Each test case follows a similar pattern: reset the environment, prepare SQL statements, execute queries using cursors or direct execution, fetch results, clean up resources, and print results. The function also demonstrates advanced ECPG features like descriptor usage for dynamic SQL statement analysis and result fetching.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [reset](../r/reset.md) (resets test environment and variables)
  - [printResult](../p/printResult.md) (displays test case results)
  - [ECPGprepare](../E/ECPGprepare.md) (prepares SQL statements)
  - [ECPGdo](../E/ECPGdo.md) (executes SQL commands)
  - [ECPGdeallocate](../E/ECPGdeallocate.md) (deallocates prepared statements)
  - [ECPGprepared_statement](../E/ECPGprepared_statement.md) (references prepared statements)
  - [ECPGallocate_desc](../E/ECPGallocate_desc.md) (allocates SQL descriptors)
  - [ECPGdeallocate_desc](../E/ECPGdeallocate_desc.md) (deallocates SQL descriptors)
  - [ECPGdescribe](../E/ECPGdescribe.md) (describes prepared statements)
  - [ECPGget_desc_header](../E/ECPGget_desc_header.md) (gets descriptor header information)
  - [ECPGget_desc](../E/ECPGget_desc.md) (gets descriptor field information)
  - Various ECPG type and constant definitions (ECPGt_char, ECPG_NOT_FOUND, etc.)
- Called from:
  - [main](../m/main.md) (in src/interfaces/ecpg/test/expected/sql-declare.c:184)

## Notes and Other Information
- This function is part of the ECPG test suite, specifically testing SQL DECLARE functionality
- Tests four scenarios: default connection usage, non-default connection (con1), direct execution without cursors, and explicit connection specification (con2)
- Demonstrates proper resource management with statement preparation, cursor handling, and cleanup
- Uses global arrays f1, f2, f3 to store query results and ARRAY_SIZE constant for array bounds
- Includes error handling through sqlca.sqlcode checks and sqlprint() calls
- The final part of the function demonstrates descriptor usage for dynamic SQL analysis, showing count, length, and data extraction
- All SQL operations are performed through ECPG interface functions rather than direct database calls
- The function is located in src/interfaces/ecpg/test/expected/sql-declare.c:216-580

## Simplified Source

```c
void execute_test(void) {
    int i, count, length;
    char *selectString = "SELECT f1,f2,f3 FROM source";

    // Test case 1: Default connection with prepared statements and cursors
    reset();
    ECPGprepare(__LINE__, NULL, 0, "stmt_1", selectString);
    ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "declare cur_1 cursor for $1",
           ECPGt_char_variable,(ECPGprepared_statement(NULL, "stmt_1", __LINE__)));

    // Fetch all rows using cursor
    i = 0;
    while (1) {
        ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "fetch cur_1", ECPGt_EOIT,
               ECPGt_int,&(f1[i]), ECPGt_int,&(f2[i]), ECPGt_char,(f3[i]));
        if (sqlca.sqlcode == ECPG_NOT_FOUND) break;
        i++;
    }
    ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "close cur_1", ECPGt_EOIT, ECPGt_EORT);
    ECPGdeallocate(__LINE__, 0, NULL, "stmt_1");
    printResult("testcase1", 2);

    // Test case 2: Non-default connection (con1)
    reset();
    ECPGprepare(__LINE__, "con1", 0, "stmt_2", selectString);
    ECPGdo(__LINE__, 0, 1, "con1", 0, ECPGst_normal, "declare cur_2 cursor for $1",
           ECPGt_char_variable,(ECPGprepared_statement("con1", "stmt_2", __LINE__)));

    // Similar fetch loop for con1
    i = 0;
    while (1) {
        ECPGdo(__LINE__, 0, 1, "con1", 0, ECPGst_normal, "fetch cur_2", ECPGt_EOIT,
               ECPGt_int,&(f1[i]), ECPGt_int,&(f2[i]), ECPGt_char,(f3[i]));
        if (sqlca.sqlcode == ECPG_NOT_FOUND) break;
        i++;
    }
    ECPGdo(__LINE__, 0, 1, "con1", 0, ECPGst_normal, "close cur_2", ECPGt_EOIT, ECPGt_EORT);
    ECPGdeallocate(__LINE__, 0, "con1", "stmt_2");
    printResult("testcase2", 2);

    // Test case 3: Direct execution without cursors
    reset();
    ECPGprepare(__LINE__, NULL, 0, "stmt_3", selectString);
    ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_execute, "stmt_3", ECPGt_EOIT,
           ECPGt_int,(f1), ECPGt_int,(f2), ECPGt_char,(f3));
    ECPGdeallocate(__LINE__, 0, NULL, "stmt_3");
    printResult("testcase3", 2);

    // Test case 4: Explicit connection (con2)
    reset();
    ECPGprepare(__LINE__, "con2", 0, "stmt_4", selectString);
    ECPGdo(__LINE__, 0, 1, "con2", 0, ECPGst_normal, "declare cur_4 cursor for $1",
           ECPGt_char_variable,(ECPGprepared_statement("con2", "stmt_4", __LINE__)));

    // Fetch and cleanup for con2
    i = 0;
    while (1) {
        ECPGdo(__LINE__, 0, 1, "con2", 0, ECPGst_normal, "fetch cur_4", ECPGt_EOIT,
               ECPGt_int,&(f1[i]), ECPGt_int,&(f2[i]), ECPGt_char,(f3[i]));
        if (sqlca.sqlcode == ECPG_NOT_FOUND) break;
        i++;
    }
    ECPGdo(__LINE__, 0, 1, "con2", 0, ECPGst_normal, "close cur_4", ECPGt_EOIT, ECPGt_EORT);
    ECPGdeallocate(__LINE__, 0, "con2", "stmt_4");
    printResult("testcase4", 2);

    // Descriptor test: demonstrate dynamic SQL analysis
    ECPGprepare(__LINE__, "con1", 0, "stmt_desc", selectString);
    ECPGdo(__LINE__, 0, 1, "con1", 0, ECPGst_normal, "declare cur_desc cursor for $1",
           ECPGt_char_variable,(ECPGprepared_statement("con1", "stmt_desc", __LINE__)));

    // Use descriptor to analyze statement structure
    ECPGallocate_desc(__LINE__, "desc_for_describe");
    ECPGdescribe(__LINE__, 0, 0, "con1", "stmt_desc", ECPGt_descriptor, "desc_for_describe");
    ECPGget_desc_header(__LINE__, "desc_for_describe", &(count));
    ECPGget_desc(__LINE__, "desc_for_describe", 3, ECPGd_length, ECPGt_int, &(length));
    ECPGdeallocate_desc(__LINE__, "desc_for_describe");

    // Fetch using descriptor and cleanup
    ECPGallocate_desc(__LINE__, "desc_for_fetch");
    ECPGdo(__LINE__, 0, 1, "con1", 0, ECPGst_normal, "fetch cur_desc", ECPGt_EOIT,
           ECPGt_descriptor, "desc_for_fetch");
    ECPGget_desc(__LINE__, "desc_for_fetch", 3, ECPGd_data, ECPGt_char, (f3[0]));
    ECPGdeallocate_desc(__LINE__, "desc_for_fetch");
    ECPGdo(__LINE__, 0, 1, "con1", 0, ECPGst_normal, "close cur_desc", ECPGt_EOIT, ECPGt_EORT);
    ECPGdeallocate(__LINE__, 0, "con1", "stmt_desc");

    printf("****descriptor results****\n");
    printf("count: %d, length: %d, data: %s\n", count, length, f3[0]);
}
```