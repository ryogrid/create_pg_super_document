# pg_checksum_block

## Location
src/include/storage/checksum_impl.h: 146 - 186

## Overview
Computes a 32-bit checksum for a PostgreSQL page block using a parallel FNV-1a hash algorithm with multiple independent hash calculations.

## Definition


## Detailed Description
This function implements PostgreSQL's page-level checksum algorithm using a sophisticated approach that calculates multiple parallel FNV-1a hashes. The algorithm is designed to provide strong collision resistance and performance by unrolling the hash computation across 32 parallel streams. The function requires the input page to be properly aligned (at least on a 4-byte boundary) and operates on blocks of exactly BLCKSZ size.

The algorithm works in three phases:
1. Initialize 32 partial checksums with different base offsets to ensure independent hash streams
2. Process the page data in chunks, applying the FNV-1a hash function to each of the 32 parallel streams
3. Perform additional mixing rounds with zero values and combine all partial checksums using XOR

## Parameters / Member Variables
- : A pointer to a PGChecksummablePage structure representing the page data to be checksummed. Must be properly aligned and exactly BLCKSZ in size.

## Dependencies
- Functions called/Symbols referenced:
  - [PGChecksummablePage](../P/PGChecksummablePage.md) (data structure)
  - N_SUMS (constant defining number of parallel checksums)
  - CHECKSUM_COMP (macro for FNV-1a hash computation)
  - checksumBaseOffsets (array of initialization values)
- Called from (representative examples):
  - [pg_checksum_page](pg_checksum_page.md)

## Notes and Other Information
- The function is implemented as a static inline function in checksum_impl.h for performance
- Requires BLCKSZ to be compatible with the parallel algorithm structure
- Uses strict aliasing through a union structure to safely access page data as uint32 arrays
- The algorithm includes assertion checks to ensure proper page size alignment
- Two additional rounds of zero mixing provide extra cryptographic strength
- Final result combines all 32 partial checksums using XOR folding