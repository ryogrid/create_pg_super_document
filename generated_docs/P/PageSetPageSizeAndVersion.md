# PageSetPageSizeAndVersion

## Location
src/include/storage/bufpage.h: 297 - 313

## Overview
Sets both the page size and page layout version in a page header by combining them into the pd_pagesize_version field.

## Definition
static inline void PageSetPageSizeAndVersion(Page page, Size size, uint8 version)

## Detailed Description
PageSetPageSizeAndVersion initializes the pd_pagesize_version field in a page header by combining the size and version parameters. The function validates that the size parameter uses only the upper 8 bits (0xFF00 mask) and the version parameter uses only the lower 8 bits (0x00FF mask), then combines them using a bitwise OR operation. This combined approach ensures atomic setting of both values and maintains the packed format used throughout PostgreSQL.

## Parameters / Member Variables
- page: A Page pointer to the page whose header should be updated
- size: The page size value to set (must fit in upper 8 bits)
- version: The page layout version to set (must fit in lower 8 bits)

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast to access page header structure)
  - Assert (for parameter validation)
- Called from (representative examples):
  - PageInit (primary caller during page initialization)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- Includes Assert statements to validate parameter ranges during development
- The function combines both values atomically rather than setting them separately
- Used primarily during page initialization when setting up new pages
- The size parameter should already be properly masked to fit in the upper 8 bits
- The version parameter should be a valid page layout version number
- Part of the core page initialization infrastructure in PostgreSQL