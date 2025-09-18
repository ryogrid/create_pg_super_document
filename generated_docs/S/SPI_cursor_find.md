# SPI_cursor_find

## Location
src/backend/executor/spi.c: 1794 - 1805

## Overview
SPI_cursor_find is a simple SPI function that locates and returns the Portal handle for an existing open cursor by its name.

## Definition


## Detailed Description
This function provides a straightforward way to retrieve the Portal associated with a named cursor that has been previously opened through the SPI interface. It serves as a wrapper around the internal GetPortalByName function, making it accessible through the SPI API. The function is commonly used when you need to operate on a cursor that was opened earlier in the same session or transaction.

## Parameters / Member Variables
- : The name of the cursor to find (must be a null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - GetPortalByName (internal portal lookup function)
- Called from (representative examples):
  - cursor_to_xml (XML generation functions)
  - cursor_to_xmlschema (XML schema functions)
  - plperl_spi_fetchrow (PL/Perl cursor operations)
  - plperl_spi_cursor_close (PL/Perl cursor cleanup)

## Notes and Other Information
- Returns NULL if no cursor with the specified name exists
- The cursor name comparison is case-sensitive
- This function does not modify the cursor state - it only provides access to the Portal handle
- The returned Portal can be used with other SPI cursor functions like SPI_cursor_fetch
- Part of the public SPI API and commonly used by procedural languages and extensions