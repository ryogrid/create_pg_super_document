# plperl_spi_cursor_close

## Location
[src/pl/plperl/plperl.c:3551-3566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3551-L3566)

## Overview
Explicitly closes an SPI cursor and releases its resources, allowing manual cleanup of cursors before they reach end-of-data.

## Definition
```c
void plperl_spi_cursor_close(char *cursor)
```

## Detailed Description
This function provides PL/Perl with the ability to manually close SPI cursors before they are automatically closed by reaching end-of-data. It performs the necessary cleanup operations to properly release cursor resources and free associated memory.

The function performs a simple but important sequence:
1. Validates SPI usage is allowed in the current context
2. Locates the portal using the provided cursor name
3. If the cursor exists, unpins the portal to remove the reference count
4. Closes the cursor using SPI_cursor_close to free all associated resources

Unlike the automatic closure that occurs in plperl_spi_fetchrow when no more rows are available, this function allows explicit control over cursor lifecycle, which can be important for resource management in long-running functions or when dealing with large result sets.

## Parameters / Member Variables
- `cursor`: C string containing the name of the SPI cursor to close. This should be a cursor name returned from a previous plperl_spi_query call.

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [SPI_cursor_find](../S/SPI_cursor_find.md)
  - [UnpinPortal](../U/UnpinPortal.md)
  - [SPI_cursor_close](../S/SPI_cursor_close.md)
- Called from (representative examples):
  - PL_PERL_H header (src/pl/plperl/plperl.h:36)

## Notes and Other Information
- Safe to call on non-existent cursors - simply does nothing if cursor is not found
- Does not use subtransaction isolation unlike query and fetch operations
- Essential for proper resource management in functions that open multiple cursors
- Should be called when finished with a cursor to prevent resource leaks
- Closing a cursor that has already been automatically closed is harmless
- No error handling beyond SPI usage validation - cursor not found is silently ignored
- Complementary to the automatic cleanup performed in plperl_spi_fetchrow