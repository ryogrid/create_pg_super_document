# SpGistInitPage

## Location
[src/backend/access/spgist/spgutils.c:700-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L700-L713)

## Overview
Initializes an SP-GiST page to an empty state with specified flags, setting up the basic page structure and opaque data.

## Definition
```c
void SpGistInitPage(Page page, uint16 f)
```

## Detailed Description
This function initializes a raw page to be used in an SP-GiST index by setting up the basic page structure and SP-GiST-specific opaque data. It first calls the generic PostgreSQL PageInit function to establish the basic page layout, then initializes the SP-GiST-specific opaque area with the provided flags and the SP-GiST page identifier. This function serves as a foundation for creating new SP-GiST pages, whether they are leaf pages, inner pages, or other specialized page types.

The function ensures that the page has the proper SP-GiST page identification and can be correctly recognized by other SP-GiST operations. The flags parameter allows the caller to specify the page type and properties that will be stored in the opaque area.

## Parameters / Member Variables
- `page`: The raw page to be initialized as an SP-GiST page
- `f`: Flags indicating the page type and properties (e.g., leaf, inner, null-storing)

## Dependencies
- Functions called/Symbols referenced:
  - PageInit
  - SpGistPageGetOpaque
  - [SpGistPageOpaqueData](SpGistPageOpaqueData.md) (struct)
  - SPGIST_PAGE_ID (constant)
- Called from (representative examples):
  - [spgbuildempty](../s/spgbuildempty.md)
  - [SpGistInitBuffer](SpGistInitBuffer.md)
  - [SpGistInitMetapage](SpGistInitMetapage.md)

## Notes and Other Information
- This is a low-level initialization function that sets up the basic SP-GiST page structure
- The function uses BLCKSZ for the page size and allocates space for SpGistPageOpaqueData
- The SPGIST_PAGE_ID constant is used to identify pages as belonging to SP-GiST indexes
- This function is typically called when creating new pages during index construction or expansion
- The flags parameter determines page-specific behavior and is essential for proper page identification