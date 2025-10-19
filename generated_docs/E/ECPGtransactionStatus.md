# ECPGtransactionStatus

## Location
[src/interfaces/ecpg/ecpglib/misc.c:145-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L145-L159)

## Overview
Returns the current transaction status for a specified ECPG database connection.

## Definition
```c
PGTransactionStatusType ECPGtransactionStatus(const char *connection_name)
```

## Detailed Description
ECPGtransactionStatus is a wrapper function that provides access to PostgreSQL transaction status information through the ECPG (Embedded SQL in C) interface. The function retrieves the connection object associated with the given connection name and queries its transaction status using the underlying libpq PQtransactionStatus function. If the connection cannot be found, the function returns PQTRANS_UNKNOWN to indicate an indeterminate transaction state.

## Parameters / Member Variables
- `connection_name`: A string identifier for the ECPG database connection whose transaction status is to be queried

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_get_connection](../e/ecpg_get_connection.md)
  - [PQtransactionStatus](../P/PQtransactionStatus.md)
  - PQTRANS_UNKNOWN
- Called from (representative examples):
  - Referenced in ecpglib.h header file

## Notes and Other Information
- Returns PGTransactionStatusType enum value indicating current transaction state
- Handles error case gracefully by returning PQTRANS_UNKNOWN for invalid connections
- Part of the ECPG library interface for embedded SQL functionality
- Located in src/interfaces/ecpg/ecpglib/misc.c at lines 145-159

## Simplified Source

```c
PGTransactionStatusType ECPGtransactionStatus(const char *connection_name) {
    // Get the connection object by name
    const struct connection *con = ecpg_get_connection(connection_name);

    // Return unknown status if connection not found
    if (con == NULL) {
        return PQTRANS_UNKNOWN;
    }

    // Return the actual transaction status from libpq
    return PQtransactionStatus(con->connection);
}
```