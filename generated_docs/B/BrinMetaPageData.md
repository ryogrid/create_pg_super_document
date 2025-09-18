# BrinMetaPageData

## Location
src/include/access/brin_page.h: 64 - 70

## Overview
BrinMetaPageData is a structure that defines the metadata stored on BRIN index metapages, containing essential information about the BRIN index configuration and state.

## Definition
```c
typedef struct BrinMetaPageData
{
    uint32      brinMagic;
    uint32      brinVersion;
    BlockNumber pagesPerRange;
    BlockNumber lastRevmapPage;
} BrinMetaPageData;
```

## Detailed Description
BrinMetaPageData represents the core metadata for a BRIN index, stored on the metapage (typically page 0) of the index. This structure contains critical information needed to operate with the BRIN index, including version information, configuration parameters, and tracking data for the reverse map pages.

The structure ensures that essential index parameters are readily accessible and provides versioning support for backward compatibility. It tracks both the logical structure of the index (pages per range) and the physical structure (reverse map page tracking).

## Parameters / Member Variables
- `brinMagic`: Magic number used to identify and validate BRIN index files
- `brinVersion`: Version number of the BRIN index format for compatibility checking
- `pagesPerRange`: Number of heap pages summarized by each BRIN index tuple, defining the granularity of the index
- `lastRevmapPage`: Block number of the last allocated reverse map page, used for efficient reverse map management

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (PostgreSQL block number type)
- Called from (representative examples):
  - brinGetStats (in brin.c:1643, 1648)
  - brin_metapage_init (in brin_pageops.c:488, 492, 511)
  - brinRevmapInitialize (in brin_revmap.c:74, 80)
  - revmap_physical_extend (in brin_revmap.c:527, 539, 618)
  - brin_xlog_revmap_extend (in brin_xlog.c:228, 231, 246)

## Notes and Other Information
- This structure is stored on the first page (metapage) of every BRIN index
- The magic number provides a quick way to validate that a file is indeed a BRIN index
- Version tracking allows PostgreSQL to handle format changes and upgrades gracefully
- The pagesPerRange value is crucial for determining which heap blocks are covered by each BRIN tuple
- lastRevmapPage helps optimize reverse map page allocation and searching
- This metadata is essential for BRIN index operations including scanning, insertion, and maintenance