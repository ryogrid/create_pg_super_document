# ECPGget_desc_header

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:84-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L84-L107)

## Overview
Retrieves the header information (field count) from an SQL descriptor in ECPG, providing the number of columns in a prepared statement or cursor result set.

## Definition

```c
bool ECPGget_desc_header(int lineno, const char *desc_name, int *count)
```
## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) library's dynamic descriptor interface. It retrieves header information from a named SQL descriptor, specifically returning the number of fields/columns available in the descriptor's associated result set. The function handles proper SQLCA initialization, error management, and logging as part of ECPG's comprehensive embedded SQL implementation.

The function locates the specified descriptor, validates its existence, and extracts the field count using PostgreSQL's libpq PQnfields() function. It also updates the SQLCA structure to indicate successful execution.

## Parameters / Member Variables
- `lineno`: Source code line number for error reporting and debugging purposes
- `desc_name`: Name of the SQL descriptor to query for header information
- `count`: Pointer to integer where the number of fields/columns will be stored

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - [sqlca_t](../s/sqlca_t.md)
  - [ecpg_raise](../e/ecpg_raise.md)
  - ECPG_OUT_OF_MEMORY
  - ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY
  - [ecpg_init_sqlca](../e/ecpg_init_sqlca.md)
  - [ecpg_result_by_descriptor](../e/ecpg_result_by_descriptor.md)
  - [PQnfields](../P/PQnfields.md)
  - [ecpg_log](../e/ecpg_log.md)
- Called from (representative examples):
  - Various test programs in ECPG test suite
  - SQL DESCRIBE statement implementations
  - Dynamic SQL execution contexts

## Notes and Other Information
- Returns true on success, false on failure
- Automatically initializes SQLCA and handles error conditions
- Sets sqlca->sqlerrd[2] to 1 to indicate successful descriptor access  
- Provides debug logging showing the number of attributes found
- Essential component of ECPG's SQL3-compliant dynamic descriptor management
- Used extensively in ECPG test cases for descriptor validation

## Simplified Source

```c
bool ECPGget_desc_header(int lineno, const char *desc_name, int *count) {
    // Get SQLCA context for error handling
    struct sqlca_t *sqlca = ECPGget_sqlca();
    if (!sqlca) {
        ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    // Initialize SQLCA for this operation
    ecpg_init_sqlca(sqlca);

    // Find the descriptor by name and get its result set
    PGresult *ECPGresult = ecpg_result_by_descriptor(lineno, desc_name);
    if (!ECPGresult)
        return false;

    // Get the number of fields/columns in the result set
    *count = PQnfields(ECPGresult);

    // Set SQLCA to indicate successful operation
    sqlca->sqlerrd[2] = 1;

    ecpg_log("ECPGget_desc_header: found %d attributes\n", *count);
    return true;
}
```