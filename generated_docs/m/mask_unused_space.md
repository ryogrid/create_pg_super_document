# mask_unused_space

## Location
src/backend/access/common/bufmask.c: 71 - 94

## Overview
Masks the unused space in a page between pd_lower and pd_upper boundaries to ensure consistent page comparisons during WAL verification.

## Definition
```c
void mask_unused_space(Page page)
```

## Detailed Description
This function masks the unused free space area within a PostgreSQL page that lies between the pd_lower and pd_upper pointers. In PostgreSQL pages, pd_lower points to the end of the used space growing from the beginning of the page, while pd_upper points to the beginning of the used space growing from the end of the page. The space between these pointers is unused and may contain arbitrary data left over from previous operations.

Since this unused space can contain unpredictable remnant data, it must be masked out during consistency checks to prevent false mismatches between original and WAL-replayed pages. The function performs bounds checking to ensure page structure validity before masking.

## Parameters / Member Variables
- `page`: A pointer to the page whose unused space should be masked

## Dependencies  
- Functions called/Symbols referenced:
  - PageHeader (type cast for page header access)
  - SizeOfPageHeaderData (minimum valid pd_lower value)
  - MASK_MARKER (constant used to fill masked space)
  - BLCKSZ (maximum page size constant)
  - memset (memory filling function)
  - elog (error logging function)
- Called from (representative examples):
  - brin_mask (BRIN index masking)
  - gin_mask (GIN index masking)  
  - gist_mask (GiST index masking)
  - hash_mask (hash index masking)
  - heap_mask (heap page masking)
  - btree_mask (B-tree index masking)
  - spg_mask (SP-GiST index masking)
  - generic_mask (generic WAL masking)
  - seq_mask (sequence masking)

## Notes and Other Information
- The function validates page structure by checking that pd_lower ≤ pd_upper ≤ pd_special and other boundary conditions
- Unused space can contain garbage data from previous tuple deletions or page reorganizations
- This masking is critical for WAL consistency checks since unused space content is not deterministic
- The function will emit an ERROR if the page structure appears corrupted
- Uses memset to fill the entire unused region with MASK_MARKER bytes
- BLCKSZ represents the maximum block/page size in PostgreSQL