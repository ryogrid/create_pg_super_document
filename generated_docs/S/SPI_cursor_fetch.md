# SPI_cursor_fetch

## Location
[src/backend/executor/spi.c:1806-1820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1806-L1820)

## Overview
SPI_cursor_fetch retrieves rows from an open cursor in either forward or backward direction, storing the results in SPI global variables for access by the calling code.

## Definition

```c
void
SPI_cursor_fetch(Portal portal, bool forward, long count)
```
## Detailed Description
This function fetches a specified number of rows from a cursor Portal in the requested direction. It acts as a wrapper around the internal _SPI_cursor_operation function, setting up the appropriate fetch direction and destination receiver. The fetched rows are stored in the SPI result structure (SPI_tuptable) and the count is updated in SPI_processed, making them accessible to the calling code through the standard SPI result interface.

The function handles both forward and backward cursor navigation, converting the boolean direction parameter to the appropriate internal FETCH_FORWARD or FETCH_BACKWARD constants. It uses a DestSPI destination receiver to capture the query results in SPI-compatible format.

## Parameters / Member Variables
- `portal`: The Portal handle for the cursor from which to fetch rows
- `forward`: Boolean indicating fetch direction (true for forward, false for backward)
- `count`: Number of rows to fetch (positive number)
## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_cursor_operation](_SPI_cursor_operation.md) (internal cursor operation handler)
  - [CreateDestReceiver](../C/CreateDestReceiver.md) (creates destination for query results)
  - DestSPI (SPI destination receiver type)
  - FETCH_FORWARD/FETCH_BACKWARD (fetch direction constants)
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (text search query rewriting)
  - [ts_stat_sql](../t/ts_stat_sql.md) (text search statistics)
  - [cursor_to_xml](../c/cursor_to_xml.md) (XML generation from cursors)
  - [plperl_spi_fetchrow](../p/plperl_spi_fetchrow.md) (PL/Perl cursor operations)
  - [PLy_cursor_fetch](../P/PLy_cursor_fetch.md) (PL/Python cursor operations)

## Notes and Other Information
- Results are stored in global SPI variables: SPI_tuptable contains the fetched rows, SPI_processed contains the actual number of rows fetched
- The function does not validate cursor direction capabilities - attempting to fetch backward from a forward-only cursor will result in an error
- For backward fetches, the cursor must have been opened with scroll capability
- The DestSPI receiver automatically handles memory management and does not require explicit cleanup
- Count parameter behavior: positive values fetch exactly that many rows (or fewer if end of cursor is reached), while 0 typically means fetch all remaining rows

## Simplified Source

```c
void SPI_cursor_fetch(Portal portal, bool forward, long count) {
    // Perform cursor operation with appropriate direction
    _SPI_cursor_operation(portal,
                         forward ? FETCH_FORWARD : FETCH_BACKWARD,
                         count,
                         CreateDestReceiver(DestSPI));
    // DestSPI receiver handles result storage in SPI globals
}
```