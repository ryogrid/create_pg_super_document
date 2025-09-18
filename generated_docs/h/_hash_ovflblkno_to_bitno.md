# _hash_ovflblkno_to_bitno

## Location
[src/backend/access/hash/hashovfl.c:62-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L62-L111)

## Overview
Converts an overflow page block number to its corresponding bit number in the free-page bitmap, performing the inverse operation of bitno_to_blkno.

## Definition
```c
uint32 _hash_ovflblkno_to_bitno(HashMetaPage metap, BlockNumber ovflblkno)
```

## Detailed Description
This function performs the reverse conversion of bitno_to_blkno, translating an absolute block number of an overflow page back to its bit number position in the overflow page bitmap. The conversion process involves:

1. Iterating through split levels to find which split contains the given overflow block
2. Calculating the relative position within that split by subtracting the total bucket count
3. Validating that the calculated bit number falls within the valid range for that split level
4. Converting from 1-based page numbering back to 0-based bit numbering

The function includes validation to ensure the provided block number corresponds to a valid overflow page, raising an error if an invalid block number is provided.

## Parameters / Member Variables
- `metap`: Pointer to the hash index metadata page containing split point and spares information
- `ovflblkno`: Block number of the overflow page to convert to bit number

## Dependencies
- Functions called/Symbols referenced:
  - HashMetaPage (metadata structure)
  - _hash_get_totalbuckets (calculates total bucket count for a split level)
  - ereport/errcode/errmsg (error reporting)
- Called from (representative examples):
  - [_hash_freeovflpage](_hash_freeovflpage.md) (when freeing overflow pages)
  - HASHNProcs (hash index procedure definitions)

## Notes and Other Information
- This is a public function (no static qualifier) accessible to other hash index modules
- The function performs comprehensive validation and will error out if given an invalid overflow block number
- The conversion relies on the spares array which tracks overflow page counts at each split level
- Returns 0-based bit numbers consistent with bitmap indexing conventions
- Critical for maintaining consistency between physical block storage and logical bitmap representation