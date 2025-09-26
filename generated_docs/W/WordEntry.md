# WordEntry

## Location
[src/include/tsearch/ts_type.h:47-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L47-L48)

## Overview
WordEntry is a compact data structure representing individual word entries within PostgreSQL's text search vectors (tsvector), storing word position information and whether the word has positional data.

## Definition

```c
typedef struct {
 *		uint16
 *			weight:2,
 *			pos:14;
 * }
 */

typedef uint16 WordEntryPos;
```
## Detailed Description
WordEntry is a fundamental building block of PostgreSQL's full-text search functionality. It serves as a header structure for individual words within a tsvector, using bit fields to pack three pieces of information into a single 32-bit integer:

- A flag indicating whether positional information is available for the word
- The length of the word string (up to 2KB)
- The position/offset where the word data begins (up to 1MB total data size)

This compact representation is crucial for memory efficiency in text search operations, as a single tsvector may contain hundreds or thousands of word entries.

## Parameters / Member Variables
- : 1-bit flag indicating whether this word entry has associated position vectors (weight and position information)
- : 11-bit field storing the length of the word string, with a maximum of 2KB (2047 bytes)
- : 20-bit field storing the byte offset where this word's data begins within the tsvector's data area, allowing up to 1MB total data

## Dependencies
- Functions called/Symbols referenced:
  - None (struct definition only)
- Used by (representative examples):
  - tsvectorin (parsing text search vectors)
  - tsvectorout (converting to text representation)
  - tsvector_concat (combining search vectors)
  - calc_rank_and/calc_rank_or (ranking calculations)
  - gin_extract_tsvector (GIN indexing)

## Notes and Other Information
- The bit field packing allows efficient storage while maintaining fast access to metadata
- Maximum word length is constrained by the 11-bit len field (MAXSTRLEN = 2047 bytes)
- Maximum total data size is limited by the 20-bit pos field (MAXSTRPOS = 1048575 bytes)
- WordEntry arrays are typically sorted by word string for binary search operations
- Position data (when haspos=1) follows the word string in the data area