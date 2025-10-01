# PageGetPageSize

## Location
[src/include/storage/bufpage.h:274-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L274-L283)

## Overview
Retrieves the page size from a formatted page header, extracting the size information from the pd_pagesize_version field.

## Definition

```c
static inline Size
PageGetPageSize(Page page)
```
## Detailed Description
PageGetPageSize extracts the page size from a formatted page by reading the pd_pagesize_version field in the page header and masking out the version bits. The function performs a bitwise AND operation with 0xFF00 to isolate the upper 8 bits which contain the page size information. This function can only be called on formatted pages (unlike BufferGetPageSize which works on unformatted pages), but it can work on pages not stored in a buffer.

## Parameters / Member Variables
- : A Page pointer to the formatted page from which to extract the size information

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast to access page header structure)
- Called from (representative examples):
  - [dataSplitPageInternal](../d/dataSplitPageInternal.md)
  - [entrySplitPage](../e/entrySplitPage.md)
  - [_bt_singleval_fillfactor](../b/_bt_singleval_fillfactor.md)
  - [_bt_findsplitloc](../b/_bt_findsplitloc.md)
  - [PageGetTempPage](PageGetTempPage.md)
  - [PageGetTempPageCopy](PageGetTempPageCopy.md)
  - [PageGetSpecialSize](PageGetSpecialSize.md)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- The page size is stored in the upper 8 bits of the pd_pagesize_version field
- Requires the page to be properly formatted with a valid PageHeader
- Used extensively in btree, GIN, and hash index operations for page management
- Part of the core page layout infrastructure in PostgreSQL storage system

## Simplified Source

```c
static inline Size
PageGetPageSize(Page page)
{
    // Extract page size from upper 8 bits of pd_pagesize_version field
    return (Size) (((PageHeader) page)->pd_pagesize_version & (uint16) 0xFF00);
}
```