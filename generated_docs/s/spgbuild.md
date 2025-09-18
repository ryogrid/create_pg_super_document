# spgbuild

## Location
src/backend/access/spgist/spginsert.c: 73 - 153

## Overview
The main function responsible for building a complete SP-GiST index from scratch by initializing the index structure and inserting all heap tuples.

## Definition


## Detailed Description
This function implements the complete SP-GiST index building process. It first validates that the index is empty, then initializes the fundamental index pages (metapage, root page, and null-tuples page). After setting up the basic structure, it scans all heap tuples using table_index_build_scan() with spgistBuildCallback() to insert each tuple into the index. The function handles WAL logging requirements and returns statistics about the build process. It uses a temporary memory context during the build to manage memory efficiently.

## Parameters / Member Variables
- : The heap relation being indexed
- : The SP-GiST index relation being built
- : Index metadata and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - SpGistNewBuffer
  - SpGistInitMetapage
  - SpGistInitBuffer
  - initSpGistState
  - table_index_build_scan
  - spgistBuildCallback
  - SpGistUpdateMetaPage
  - RelationNeedsWAL
  - log_newpage_range
- Called from (representative examples):
  - spghandler

## Notes and Other Information
The function ensures index pages are created in the correct order (meta, root, null) and validates their block numbers. It includes comprehensive WAL logging for durability when required. The build process is atomic and creates a fully functional SP-GiST index ready for queries.