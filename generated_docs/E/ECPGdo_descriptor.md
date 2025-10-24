# ECPGdo_descriptor

## Location
[src/interfaces/ecpg/ecpglib/execute.c:2292-2298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L2292-L2298)

## Overview
A legacy interface function for executing SQL statements using descriptor-based parameter handling in the ECPG library.

## Definition
bool ECPGdo_descriptor(int line, const char *connection, const char *descriptor, const char *query)

## Detailed Description
ECPGdo_descriptor provides a simplified interface for executing SQL statements that use descriptor-based parameter and result handling. This function is marked as an "old descriptor interface" and serves as a compatibility wrapper around the more general ECPGdo function. It automatically configures the execution with PostgreSQL compatibility mode, forces indicator handling, and sets up the descriptor-based parameter handling with appropriate ECPG type markers. The function is designed for cases where SQL descriptors are used to define the structure and types of input/output parameters.

## Parameters / Member Variables
- line: Line number in the source code where the SQL statement appears (for error reporting)
- connection: Name of the database connection to use
- descriptor: Name of the SQL descriptor containing parameter/result definitions
- query: The SQL query string to execute

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGdo](ECPGdo.md)
  - ECPG_COMPAT_PGSQL
  - ECPGt_descriptor
  - ECPGt_EOIT
  - ECPGt_NO_INDICATOR
  - ECPGt_EORT
- Called from (representative examples):
  - (Referenced in ecpglib.h header file)

## Notes and Other Information
- Returns true on successful execution, false on failure
- This is a legacy/compatibility interface for descriptor-based SQL execution
- Automatically uses PostgreSQL compatibility mode (ECPG_COMPAT_PGSQL)
- Forces indicator variable handling (true parameter)
- Sets up standard descriptor execution with end-of-input/end-of-result markers
- Located in src/interfaces/ecpg/ecpglib/execute.c:2292-2298
- The function simplifies descriptor usage by providing sensible defaults for most parameters

## Simplified Source

```c
bool ECPGdo_descriptor(int line, const char *connection,
                       const char *descriptor, const char *query) {
    // Legacy wrapper for descriptor-based SQL execution
    // Delegates to ECPGdo with PostgreSQL compatibility settings
    return ECPGdo(line, ECPG_COMPAT_PGSQL, true, connection, '\0', 0, query,
                  ECPGt_EOIT,                      // End of input marker
                  ECPGt_descriptor, descriptor,    // Use SQL descriptor
                  0L, 0L, 0L,                     // Descriptor parameters
                  ECPGt_NO_INDICATOR, NULL,       // No indicator variables
                  0L, 0L, 0L,
                  ECPGt_EORT);                    // End of result marker
}
```