# GinInitMetabuffer

## Location
src/backend/access/gin/ginutil.c: 356 - 387

## Overview
Initializes a GIN metapage buffer with default metadata values, setting up the essential control information for a GIN index.

## Definition
```c
void GinInitMetabuffer(Buffer b)
```

## Detailed Description
GinInitMetabuffer specializes in initializing the metapage of a GIN index, which contains critical metadata about the entire index structure. The function first calls GinInitPage with the GIN_META flag to establish the basic page structure, then populates the GinMetaPageData structure with initial values.

The metapage serves as the control center for the GIN index, tracking statistics like the number of pages, entries, and pending tuples. It also maintains pointers to the fast insertion list (head/tail) and version information. The function ensures that the metadata is properly positioned on the page by setting pd_lower to prevent the metadata from being lost during WAL compression.

This function is essential during index creation and restoration operations, establishing the foundation for all subsequent GIN operations.

## Parameters / Member Variables
- `b`: Buffer containing the page to be initialized as a GIN metapage

## Dependencies
- Functions called/Symbols referenced:
  - [GinInitPage](GinInitPage.md): Core page initialization with GIN_META flag
  - [BufferGetPage](../B/BufferGetPage.md): Extracts page pointer from buffer
  - [BufferGetPageSize](../B/BufferGetPageSize.md): Gets page size from buffer
  - GinPageGetMeta: Retrieves metadata structure from the page
  - [GinMetaPageData](GinMetaPageData.md): Structure containing all GIN index metadata
  - GIN_META: Flag constant identifying this as a metapage
  - GIN_CURRENT_VERSION: Current version number for GIN indexes
  - PageHeader: PostgreSQL page header structure

- Called from (representative examples):
  - [ginbuild](../g/ginbuild.md): During initial index construction
  - [ginbuildempty](../g/ginbuildempty.md): When creating empty GIN indexes
  - [ginRedoUpdateMetapage](../g/ginRedoUpdateMetapage.md): During WAL replay for metapage updates
  - [ginRedoDeleteListPages](../g/ginRedoDeleteListPages.md): During WAL replay when cleaning up list pages

## Notes and Other Information
- The metapage is always block 0 of a GIN index
- All counters and pointers are initialized to zero/invalid values, representing an empty index
- The pd_lower setting is crucial to prevent metadata loss during WAL compression
- The function sets up tracking for both the main index structure and the fast insertion list
- Version information is stored to handle compatibility across PostgreSQL versions
- Fast insertion list pointers (head/tail) are initialized to InvalidBlockNumber
- Statistics counters (nTotalPages, nEntryPages, nDataPages, nEntries) start at zero