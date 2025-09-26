# pg_unicode_decomposition

## Location
[src/include/common/unicode_norm_table.h:26-27](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_norm_table.h#L26-L27)

## Overview
A structure that represents a Unicode character's decomposition information, storing the codepoint, combining class, and decomposition data for Unicode normalization operations.

## Definition

```c
* decomposition itself if DECOMP_INLINE */
} pg_unicode_decomposition;

#define DECOMP_NO_COMPOSE	0x80	/* don't use for re-composition */
#define DECOMP_INLINE		0x40	/* decomposition is stored inline in
									 * dec_index */
#define DECOMP_COMPAT		0x20	/* compatibility mapping */

#define DECOMPOSITION_SIZE(x) ((x)->dec_size_flags & 0x1F)
#define DECOMPOSITION_NO_COMPOSE(x) (((x)->dec_size_flags & (DECOMP_NO_COMPOSE | DECOMP_COMPAT)) != 0)
#define DECOMPOSITION_IS_INLINE(x) (((x)->dec_size_flags & DECOMP_INLINE) != 0)
#define DECOMPOSITION_IS_COMPAT(x) (((x)->dec_size_flags & DECOMP_COMPAT) != 0)

/* Table of Unicode codepoints and their decompositions */
static const pg_unicode_decomposition UnicodeDecompMain[6775] =
```
## Detailed Description
The  structure is a fundamental data type used in PostgreSQL's Unicode normalization system. It serves as an entry in lookup tables that provide decomposition information for Unicode characters according to the Unicode Standard's normalization forms (NFD, NFC, NFKD, NFKC).

This structure is part of PostgreSQL's implementation of Unicode normalization as defined in Unicode Standard Annex #15 (UAX #15). Each entry represents a single Unicode codepoint that has either decomposition mappings or specific combining class information required for normalization processing.

The structure is designed to be compact and efficient for lookup operations, supporting both backend (using perfect hash functions) and frontend (using binary search) implementations. The decomposition data can be stored either inline in the  field for single-character decompositions or as an index to a separate array for multi-character decompositions.

## Parameters / Member Variables
- : The Unicode codepoint (32-bit value) that this entry represents
- : The canonical combining class of the character (0-255), used for proper reordering during normalization
- : A packed field containing both the size of the decomposition sequence (lower 5 bits) and control flags (upper 3 bits) indicating decomposition properties
- : Either an index into the UnicodeDecomp_codepoints array for multi-character decompositions, or the decomposed character itself when DECOMP_INLINE flag is set

## Dependencies
- Functions called/Symbols referenced:
  - DECOMPOSITION_SIZE (macro)
  - DECOMPOSITION_NO_COMPOSE (macro) 
  - DECOMPOSITION_IS_INLINE (macro)
  - DECOMPOSITION_IS_COMPAT (macro)
  - UnicodeDecomp_codepoints (external array)

- Called from (representative examples):
  - [conv_compare](../c/conv_compare.md)
  - [get_code_entry](../g/get_code_entry.md)
  - [get_canonical_class](../g/get_canonical_class.md)
  - [get_code_decomposition](../g/get_code_decomposition.md)
  - [get_decomposed_size](../g/get_decomposed_size.md)
  - [recompose_code](../r/recompose_code.md)
  - [decompose_code](../d/decompose_code.md)

## Notes and Other Information
- The structure is defined in an auto-generated header file () created by the  script
- The  field uses bit manipulation to pack multiple pieces of information:
  - Bits 0-4: Decomposition sequence length (max 31 characters)
  - Bit 5 (DECOMP_COMPAT): Indicates compatibility mapping vs canonical mapping
  - Bit 6 (DECOMP_INLINE): Indicates decomposition is stored inline in dec_index
  - Bit 7 (DECOMP_NO_COMPOSE): Indicates character should not be used for recomposition
- The main lookup table  contains 6775 entries covering all Unicode characters with decompositions or non-zero combining classes
- [Backend](../B/Backend.md) and frontend implementations use different lookup strategies: perfect hash function vs binary search respectively
- This structure is fundamental to PostgreSQL's text processing capabilities for proper Unicode handling in international applications