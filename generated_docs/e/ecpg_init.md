# ecpg_init

## Location
[src/interfaces/ecpg/test/pg_regress_ecpg.c:254-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/pg_regress_ecpg.c#L254-L259)

## Overview
Initializes ECPG (Embedded SQL in C) context by validating the SQL Communication Area (SQLCA) and database connection, setting up the foundation for subsequent ECPG operations.

## Definition
```c
bool ecpg_init(const struct connection *con, const char *connection_name, const int lineno)
```

## Detailed Description
This function serves as a fundamental initialization routine for ECPG operations. It performs essential validation and setup tasks required before executing embedded SQL statements:

1. **SQLCA Acquisition**: Retrieves the current thread's SQL Communication Area using ECPGget_sqlca()
2. **Memory Validation**: Ensures the SQLCA is properly allocated and accessible
3. **SQLCA Initialization**: Calls ecpg_init_sqlca() to reset SQLCA fields to their default state
4. **Connection Validation**: Verifies that a valid database connection is provided

The function implements proper error handling by raising appropriate ECPG errors with line number information for debugging purposes. It returns a boolean status indicating success or failure, allowing calling code to handle initialization failures gracefully.

## Parameters / Member Variables
- `con`: Pointer to the database connection structure to validate
- `connection_name`: Name of the connection (used for error reporting, can be NULL)
- `lineno`: Source code line number where the initialization was called (for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca (retrieves current SQLCA)
  - [ecpg_init_sqlca](ecpg_init_sqlca.md) (initializes SQLCA fields)
  - [ecpg_raise](ecpg_raise.md) (error reporting mechanism)
  - [ecpg_gettext](ecpg_gettext.md) (internationalization support)
- Error constants:
  - ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY
  - ECPG_NO_CONN, ECPG_SQLSTATE_CONNECTION_DOES_NOT_EXIST
- Called from (examples):
  - [ECPGsetcommit](../E/ECPGsetcommit.md) (transaction control)
  - [ECPGsetconn](../E/ECPGsetconn.md) (connection management)
  - [ECPGdisconnect](../E/ECPGdisconnect.md) (connection termination)
  - [ecpg_do_prologue](ecpg_do_prologue.md) (SQL execution setup)
  - [ECPGstatus](../E/ECPGstatus.md), ECPGtrans (various ECPG operations)
  - [ECPGprepare](../E/ECPGprepare.md), ECPGdeallocate (prepared statement management)

## Notes and Other Information
- This is a public function in the ECPG library interface
- Essential prerequisite for most ECPG database operations
- Provides comprehensive error handling with internationalized messages
- The lineno parameter enables precise error location reporting in embedded SQL code
- Thread-safe operation through ECPGget_sqlca() which handles per-thread SQLCA management
- Returns false on any validation failure, true on successful initialization
- Part of the core ECPG runtime library located at src/interfaces/ecpg/ecpglib/misc.c:73-95

## Simplified Source

```c
bool ecpg_init(const struct connection *con, const char *connection_name, const int lineno) {
    // Get the SQL Communication Area for this thread
    struct sqlca_t *sqlca = ECPGget_sqlca();

    // Check if SQLCA is available
    if (sqlca == NULL) {
        ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    // Initialize the SQLCA to default state
    ecpg_init_sqlca(sqlca);

    // Verify database connection is valid
    if (con == NULL) {
        ecpg_raise(lineno, ECPG_NO_CONN, ECPG_SQLSTATE_CONNECTION_DOES_NOT_EXIST,
                   connection_name ? connection_name : ecpg_gettext("NULL"));
        return false;
    }

    return true;
}
```