# AcceptResult

## Location
[src/bin/psql/common.c:403-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L403-L460)

## Overview
AcceptResult validates a PostgreSQL query result and ensures the database connection remains active, providing error handling for various result states.

## Definition

```c
static bool
AcceptResult(const PGresult *result, bool show_error)
```
## Detailed Description
AcceptResult is a static function in psql that serves as a centralized result validation mechanism. It examines the status of a PGresult object returned from PostgreSQL query execution and determines whether the result represents a successful operation or an error condition. The function handles multiple valid result states (successful commands, tuples, empty queries, and copy operations) and distinguishes them from error states (bad responses, non-fatal errors, and fatal errors). When an error is detected and error display is enabled, the function retrieves and logs the error message from the database connection and checks the connection status.

## Parameters / Member Variables
- `*result`: Pointer to the PGresult structure returned from a PostgreSQL query execution
- `show_error`: Boolean flag indicating whether error messages should be displayed to the user when validation fails
## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](../P/PQresultStatus.md) (PostgreSQL libpq function)
  - [PQerrorMessage](../P/PQerrorMessage.md) (PostgreSQL libpq function)
  - pg_log_info (PostgreSQL logging function)
  - pg_log_error (PostgreSQL logging function)
  - [CheckConnection](../C/CheckConnection.md) (psql connection validation function)
- [Result](../R/Result.md) status constants:
  - PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_TUPLES_CHUNK
  - PGRES_EMPTY_QUERY, PGRES_COPY_IN, PGRES_COPY_OUT
  - PGRES_BAD_RESPONSE, PGRES_NONFATAL_ERROR, PGRES_FATAL_ERROR
- Called from:
  - [PSQLexec](../P/PSQLexec.md) (src/bin/psql/common.c:655)
  - [DescribeQuery](../D/DescribeQuery.md) (src/bin/psql/common.c:1348, 1396)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1532, 1747)

## Notes and Other Information
This function is fundamental to psql's error handling strategy, providing a consistent way to validate query results across different execution contexts. It distinguishes between acceptable result states (including successful operations and expected conditions like empty queries) and various error conditions. The function's design allows callers to control whether error messages are displayed, enabling silent validation when appropriate. The connection check performed after error detection helps maintain session integrity by verifying the database connection is still functional.

## Simplified Source

```c
static bool
AcceptResult(const PGresult *result, bool show_error)
{
    bool is_valid;

    // Check for null result
    if (!result) {
        is_valid = false;
    } else {
        // Evaluate result status
        switch (PQresultStatus(result)) {
            case PGRES_COMMAND_OK:
            case PGRES_TUPLES_OK:
            case PGRES_TUPLES_CHUNK:
            case PGRES_EMPTY_QUERY:
            case PGRES_COPY_IN:
            case PGRES_COPY_OUT:
                // These are all acceptable result states
                is_valid = true;
                break;

            case PGRES_BAD_RESPONSE:
            case PGRES_NONFATAL_ERROR:
            case PGRES_FATAL_ERROR:
                // Known error states
                is_valid = false;
                break;

            default:
                // Unexpected status code
                is_valid = false;
                pg_log_error("unexpected PQresultStatus: %d", PQresultStatus(result));
                break;
        }
    }

    // Handle error display and connection check
    if (!is_valid && show_error) {
        const char *error = PQerrorMessage(pset.db);

        if (strlen(error))
            pg_log_info("%s", error);

        CheckConnection();
    }

    return is_valid;
}
```