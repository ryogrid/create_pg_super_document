# GISTPageOpaqueData

## Location
src/include/access/gist.h: 77 - 83

## Overview
GISTPageOpaqueData is a structure that defines the opaque data stored in each page of a GiST (Generalized Search Tree) index, containing metadata necessary for page management and navigation.

## Definition


## Detailed Description
GISTPageOpaqueData serves as the page header structure for GiST index pages, providing essential metadata for page management, navigation, and consistency checking. The structure is stored in the opaque area of each page and contains information needed for proper GiST index operations including page linking, split detection, and page type identification.

The NSN (Node Sequence Number) is a critical component used for detecting concurrent page splits during index traversal. It acts as a special-purpose LSN that is updated only during page splits, allowing the system to detect when a page has been split since a search began.

## Parameters / Member Variables
- : PageGistNSN - Node Sequence Number that must change on page split, used for detecting concurrent splits during index traversal
- : BlockNumber - Points to the next page at the same level (for leaf pages) or invalid block number if this is the rightmost page
- : uint16 - Bit flags indicating page properties:
  - F_LEAF (1 << 0): Indicates this is a leaf page
  - F_DELETED (1 << 1): The page has been deleted
  - F_TUPLES_DELETED (1 << 2): Some tuples on the page were deleted
  - F_FOLLOW_RIGHT (1 << 3): Page to the right has no downlink
  - F_HAS_GARBAGE (1 << 4): Some tuples on the page are dead but not deleted yet
- : uint16 - Identifier used for verification that this is indeed a GiST index page

## Dependencies
- Functions called/Symbols referenced:
  - PageGistNSN
  - BlockNumber
- Called from (representative examples):
  - gistInitBuffering
  - calculatePagesPerBuffer
  - gistinitpage
  - gistcheckpage
  - GISTPageOpaque (type alias)

## Notes and Other Information
- The structure is aliased as GISTPageOpaque pointer type for convenient access
- The NSN mechanism is crucial for handling concurrent page splits in multi-user environments
- The gist_page_id field serves as a sanity check to ensure page corruption hasn't occurred
- The rightlink field implements a linked list structure at each level of the index tree
- Flag bits provide efficient storage of boolean page properties in a single 16-bit field