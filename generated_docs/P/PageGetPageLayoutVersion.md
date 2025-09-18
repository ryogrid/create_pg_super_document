# PageGetPageLayoutVersion

## Location
src/include/storage/bufpage.h: 284 - 296

## Overview
Retrieves the page layout version from a formatted page header, extracting version information from the pd_pagesize_version field.

## Definition
static inline uint8 PageGetPageLayoutVersion(Page page)

## Detailed Description
PageGetPageLayoutVersion extracts the page layout version from a formatted page by reading the pd_pagesize_version field in the page header and masking out the size bits. The function performs a bitwise AND operation with 0x00FF to isolate the lower 8 bits which contain the page layout version information. This version information indicates the format and structure version of the page layout.

## Parameters / Member Variables
- page: A Page pointer to the formatted page from which to extract the layout version information

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast to access page header structure)
- Called from (representative examples):
  - Currently no direct callers found in the codebase

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- The page layout version is stored in the lower 8 bits of the pd_pagesize_version field
- Requires the page to be properly formatted with a valid PageHeader  
- The version information helps PostgreSQL understand how to interpret the page structure
- Part of the core page layout infrastructure but appears to be unused in current codebase
- Provides forward compatibility for potential future page format changes