# _SPI_checktuples

## Location
src/backend/executor/spi.c: 3117 - 3140

## Overview
_SPI_checktuples is a static internal function that validates the consistency between the number of processed tuples and the tuple table contents after a SPI operation.

## Definition

```c
static bool
_SPI_checktuples(void)
```
## Detailed Description
This function performs a critical validation check to ensure data consistency after a SPI operation has completed. It verifies that the number of tuples reported as processed matches the actual number of tuples stored in the SPI tuple table.

The function examines two key pieces of state from the current SPI connection: the processed count (number of tuples that the operation claims to have processed) and the tuple table (the actual storage structure containing the result tuples). It returns true if an inconsistency is detected, indicating a failure condition.

Two specific failure conditions are checked: if the tuple table is NULL (indicating that spi_dest_startup was not properly called during the operation setup), or if the processed count doesn't match the number of values actually stored in the tuple table.

## Parameters / Member Variables
This function takes no parameters and operates on global SPI state.

## Dependencies
- Functions called/Symbols referenced:
  - SPITupleTable: Structure type used to store SPI result tuples
  - SPIPlanPtr: Referenced in the same source region (context reference)
- Called from (representative examples):
  - _SPI_pquery: Main SPI query processing function
  - _SPI_cursor_operation: Cursor operation handler

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Returns true to indicate failure/inconsistency, false for success (note the inverted logic)
- The check for NULL tuple table detects cases where the SPI destination startup was not properly executed
- The processed count comparison ensures that the reported results match the actual stored results
- This validation helps detect internal SPI implementation bugs and ensures data integrity
- Used as a safety check after SPI operations complete to verify internal consistency