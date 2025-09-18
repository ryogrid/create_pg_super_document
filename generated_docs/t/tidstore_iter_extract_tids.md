# tidstore_iter_extract_tids

## Location
src/backend/access/common/tidstore.c: 580 - 622

## Overview
A static helper function that extracts tuple identifiers (TIDs) from a BlocktableEntry and populates the iterator's output structure with the corresponding block number and offset numbers.

## Definition


## Detailed Description
This function processes a BlocktableEntry (which represents a page in the TidStore's radix tree) to extract all TID offset numbers for a given block. It handles two different storage formats:

1. **Header-based storage** (when nwords == 0): Offsets are stored directly in the header's full_offsets array for efficiency when there are few offsets.

2. **Bitmap-based storage** (when nwords > 0): Offsets are represented as bits in a bitmap, where each set bit corresponds to a valid offset number. The function iterates through each bitmap word, checking individual bits and converting set bit positions to offset numbers.

The function dynamically expands the output buffer if needed when processing bitmap words to ensure sufficient space for all extracted offsets.

## Parameters / Member Variables
- : Pointer to the TidStoreIter containing the output structure to populate
- : Block number associated with the TIDs being extracted
- : Pointer to the BlocktableEntry containing the TID offset data

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIter (iterator structure type)
  - BlocktableEntry (page entry structure type) 
  - TidStoreIterResult (output structure type)
  - NUM_FULL_OFFSETS (constant for header array size)
  - InvalidOffsetNumber (constant representing invalid offset)
  - bitmapword (bitmap word type)
  - BITS_PER_BITMAPWORD (constant for bits per word)
  - repalloc (memory reallocation function)
- Called from (representative examples):
  - TidStoreIterateNext (main iteration function that processes each page)

## Notes and Other Information
- This is a static function, only accessible within the tidstore.c file
- Optimizes for two different TID storage patterns: sparse (header) vs dense (bitmap)
- The bitmap processing uses bit manipulation to efficiently extract set bit positions
- Dynamically grows the output offset array by doubling its size when capacity is exceeded
- Sets result->num_offsets to track the count of extracted offsets
- The function assumes the caller has already allocated the initial output buffer