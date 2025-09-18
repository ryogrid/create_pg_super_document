# brin_mask

## Location
src/backend/access/brin/brin_xlog.c: 342 - 367

## Overview
Masks a BRIN page before consistency checks by removing non-essential data that may vary between primary and standby servers during replication.

## Definition


## Detailed Description
This function prepares a BRIN page for consistency checking by masking (zeroing out or normalizing) data that is expected to differ between primary and standby servers during logical replication or other consistency verification processes. The function performs several masking operations:

1. **LSN and Checksum Masking**: Removes page LSN and checksum information using standard page masking utilities
2. **Hint Bits Masking**: Masks page hint bits that may vary between servers
3. **Unused Space Masking**: For regular BRIN pages and properly initialized meta pages, masks unused space that may contain garbage data
4. **Evacuation Flag Masking**: Removes the BRIN_EVACUATE_PAGE flag since it's not WAL-logged and irrelevant for consistency checks

The function is specifically designed to handle both regular BRIN pages and meta pages, applying appropriate masking strategies for each page type.

## Parameters / Member Variables
- : Character pointer to the raw page data that needs to be masked for consistency checking
- : Block number of the page being masked (for potential future use or debugging purposes)

## Dependencies
- Functions called/Symbols referenced:
  - mask_page_lsn_and_checksum: Standard function to mask LSN and checksum
  - mask_page_hint_bits: Standard function to mask hint bits
  - mask_unused_space: Standard function to mask unused page space
  - BRIN_IS_REGULAR_PAGE: Macro to check if page is a regular BRIN page
  - BRIN_IS_META_PAGE: Macro to check if page is a BRIN meta page
  - BrinPageFlags: Macro to access BRIN page flags
  - SizeOfPageHeaderData: Constant defining page header size
- Called from (representative examples):
  - PostgreSQL consistency checking infrastructure during replication verification

## Notes and Other Information
- This is a public function used by PostgreSQL's consistency checking framework
- Essential for logical replication and standby server verification processes
- The function handles different BRIN page types (regular and meta) with type-specific masking logic
- The BRIN_EVACUATE_PAGE flag is specifically masked because it's a runtime optimization flag not reflected in WAL
- Part of PostgreSQL's broader page masking infrastructure for replication consistency
- The blkno parameter is currently unused but included for consistency with the page masking interface