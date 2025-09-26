# SPI_start_transaction

## Location
[src/backend/executor/spi.c:222-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L222-L226)

## Overview
SPI_start_transaction is a deprecated no-op function maintained for backwards compatibility, as SPI callers are always already within a transaction context.

## Definition

```c
void
SPI_start_transaction(void)
```
## Detailed Description
SPI_start_transaction is an empty function that performs no operations. It exists solely for backwards compatibility with older code that may have used this function when it had actual functionality. 

In modern PostgreSQL, SPI callers are always executing within an active transaction context, making explicit transaction start operations unnecessary and potentially harmful. The PostgreSQL transaction system automatically manages transaction state, and SPI operations participate in the current transaction automatically.

The function is effectively:


This design reflects PostgreSQL's evolution toward better transaction management where explicit transaction control by SPI clients is generally not needed in atomic mode connections.

## Parameters / Member Variables
This function takes no parameters and has no member variables.

## Dependencies
- Functions called/Symbols referenced: None
- Called from: Limited usage, primarily in legacy code or documentation examples

## Notes and Other Information
- **Deprecated**: This function is maintained only for backwards compatibility
- **No-op**: Performs no actual operations when called
- **Transaction Context**: SPI callers are always within a transaction, making this function unnecessary
- **Modern Alternative**: For non-atomic SPI connections, use SPI_commit and SPI_rollback for transaction control
- **Safe to Call**: While deprecated, calling this function is safe as it does nothing
- **Legacy Code**: Existing code using this function will continue to work but should be updated to remove the call
- Located in src/backend/executor/spi.c:222-226