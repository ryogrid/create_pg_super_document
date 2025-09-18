# make_tsvector

## Location
src/backend/tsearch/to_tsany.c: 165 - 242

## Overview
Constructs a TSVector data structure from parsed text, creating the final binary representation used for PostgreSQL's text search functionality.

## Definition


## Detailed Description
 is responsible for creating the final TSVector data structure from a ParsedText input. This function performs several critical operations:

1. **Deduplication**: Calls  to merge duplicate words and consolidate their position information into arrays.

2. **Space calculation**: Computes the total space needed for the TSVector, including:
   - Space for WordEntry headers
   - Space for word strings
   - Space for position arrays (properly aligned)

3. **Memory allocation**: Allocates and initializes the TSVector structure using PostgreSQL's memory management.

4. **Data serialization**: Populates the TSVector with:
   - Word entries containing length, position offset, and haspos flags
   - Word strings stored in a contiguous buffer
   - Position arrays for each word (when present) with proper alignment

5. **Cleanup**: Frees all temporary memory allocated during parsing.

The function enforces size limits () and handles proper alignment requirements for position data. Each word's positions are stored with weight information (defaulting to weight 0) and position values.

## Parameters / Member Variables
- : Pointer to ParsedText structure containing the parsed words and their positions

Returns: A complete TSVector ready for storage or further processing

## Dependencies
- Functions called/Symbols referenced:
  - : Deduplicates words and consolidates positions
  - : Alignment macro for position data
  - : Calculates total size needed for TSVector
  - : PostgreSQL zero-initialized memory allocation
  - : PostgreSQL memory deallocation
  - : Sets the variable-length header size
  - : Gets pointer to WordEntry array
  - : Gets pointer to string data area
  - : Gets pointer to position data for a word
  - : Sets weight in WordEntryPos
  - : Sets position in WordEntryPos
  - : Standard C memory copy function
- Called from (representative examples):
  - : Main entry point for text-to-tsvector conversion
  - : JSON/JSONB to tsvector conversion
  - : JSON to tsvector conversion
  - : Trigger function for automatic tsvector updates

## Notes and Other Information
- This function represents the final stage of text-to-tsvector conversion
- The resulting TSVector follows PostgreSQL's internal binary format for efficient storage and searching
- Position data is aligned on 2-byte boundaries for performance
- Maximum string length is enforced to prevent excessive memory usage
- All intermediate parsing data is freed, making this function responsible for cleanup
- Located at lines 165-242 in 
- The function modifies and frees the input ParsedText structure as part of its operation