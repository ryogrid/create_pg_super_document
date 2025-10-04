# commitTable

## Location
[src/interfaces/ecpg/test/expected/sql-declare.c:581-599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/sql-declare.c#L581-L599)

## Overview
The commitTable function commits database transactions on both con1 and con2 connections in an ECPG test environment.

## Definition

```c
}

void commitTable()
```
## Detailed Description
The commitTable function is a utility function in the ECPG test suite that performs transaction commits on two specific database connections (con1 and con2). It uses the ECPGtrans function to execute COMMIT commands on both connections sequentially, ensuring that any pending transactions are properly committed to the database. This function serves as part of the test cleanup or transaction management process in the ECPG declare statement testing framework.

The function includes error handling through sqlca.sqlcode checks, calling sqlprint() if any errors occur during the commit operations. This ensures that transaction failures are properly reported during test execution.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [ECPGtrans](../E/ECPGtrans.md) (executes transaction commands on specified connections)
  - [sqlprint](../s/sqlprint.md) (prints SQL error information, called conditionally on errors)
- Called from:
  - [main](../m/main.md) (in src/interfaces/ecpg/test/expected/sql-declare.c:182 and 199)

## Notes and Other Information
- This function is part of the ECPG test suite infrastructure, specifically for the SQL DECLARE statement tests
- Commits transactions on exactly two connections: con1 and con2
- Uses ECPG's transaction management interface rather than direct SQL commands
- Includes standard ECPG error handling pattern with sqlca.sqlcode checks
- The function ensures both connections are committed even if one fails (no early return on error)
- Located in src/interfaces/ecpg/test/expected/sql-declare.c:581-599
- Called multiple times in main(), suggesting it's used for both setup and cleanup phases of testing

## Simplified Source

```c
void commitTable() {
    // Commit transaction on con1 connection
    ECPGtrans(__LINE__, "con1", "commit");
    if (sqlca.sqlcode < 0) sqlprint();

    // Commit transaction on con2 connection
    ECPGtrans(__LINE__, "con2", "commit");
    if (sqlca.sqlcode < 0) sqlprint();
}
```