# brin_metapage_init

## Location
[src/backend/access/brin/brin_pageops.c:486-523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L486-L523)

## Overview
Initializes the metapage for a new BRIN index, setting up essential metadata including magic number, version, pages per range, and revmap tracking information.

## Definition


## Detailed Description
The  function creates and initializes the metapage for a BRIN index, which is always stored as block 0 of the index relation. The metapage contains critical information needed for proper BRIN index operation and maintenance.

The function first calls  to set up the basic page structure with the  type, then populates the BRIN-specific metadata structure. This includes the magic number for identification, version information for compatibility checking, and the crucial  parameter that determines how many heap pages each BRIN tuple summarizes.

A key aspect of the initialization is setting the  to 0, which represents a bootstrap value that enables the first revmap page to be created when needed. The function also carefully sets the page's  boundary to protect the metadata from being lost during WAL compression.

## Parameters / Member Variables
- : Pointer to the page that will become the metapage
- : Number of heap pages that each BRIN tuple will summarize
- : BRIN version number for compatibility and upgrade handling

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes the basic page structure
  - : Gets pointer to the page's content area
  - : Page type constant for metapages
  - : Magic number for BRIN metapage identification
  - : Structure containing BRIN metadata
- Called from (representative examples):
  - : During initial BRIN index construction
  - : When creating empty BRIN indexes
  - : During WAL replay of index creation

## Notes and Other Information
- The metapage is always located at block number 0 of the BRIN index
- Sets  to 0 as a bootstrap value, enabling first revmap page creation
- Carefully manages  to prevent metadata loss during WAL compression
- The  parameter is fundamental to BRIN's operation and cannot be changed after index creation
- The magic number provides a way to verify that a page is indeed a BRIN metapage
- Version information enables future upgrades and compatibility checking
- This function is only called during index creation, not during normal operations
- The metapage serves as the entry point for all BRIN index operations