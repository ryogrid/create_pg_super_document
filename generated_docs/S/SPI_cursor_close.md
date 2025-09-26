# SPI_cursor_close

## Location
[src/backend/executor/spi.c:1862-1874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1862-L1874)

## Overview
Close and deallocate a cursor in PostgreSQL's Server Programming Interface (SPI), releasing all associated resources.

## Definition

```c
void
SPI_cursor_close(Portal portal)
```
## Detailed Description
SPI_cursor_close is used to properly close and deallocate a cursor (Portal) that was previously opened through the SPI interface. This function performs validation to ensure the portal is valid before calling PortalDrop to actually close and clean up the portal resources. Once a cursor is closed using this function, it cannot be used for further operations.

The function is essential for proper resource management in SPI applications, as unclosed cursors can lead to memory leaks and resource exhaustion. It should be called for every cursor that was successfully opened when the cursor is no longer needed.

## Parameters / Member Variables
- : A Portal object representing the cursor to be closed. Must be a valid, previously opened cursor.

## Dependencies
- Functions called/Symbols referenced:
  - PortalIsValid (validation function)
  - [PortalDrop](../P/PortalDrop.md) (actual portal cleanup function)
  - [Portal](../P/Portal.md) (struct type)
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md)
  - [ts_stat_sql](../t/ts_stat_sql.md)
  - [query_to_xmlschema](../q/query_to_xmlschema.md)
  - [query_to_xml_and_xmlschema](../q/query_to_xml_and_xmlschema.md)
  - [plperl_spi_cursor_close](../p/plperl_spi_cursor_close.md) (Perl procedural language)
  - [PLy_cursor_close](../P/PLy_cursor_close.md) (Python procedural language)

## Notes and Other Information
- The function throws an ERROR if an invalid portal is provided
- After calling this function, the portal should not be referenced again
- This function does not return any value or status code
- It's safe to call this function multiple times on the same portal (subsequent calls will simply do nothing as the portal becomes invalid)
- Essential for preventing resource leaks in long-running SPI applications
- Used extensively by procedural language implementations (PL/Perl, PL/Python) to manage cursor lifecycles
- The second parameter to PortalDrop is set to false, indicating normal cleanup rather than error cleanup