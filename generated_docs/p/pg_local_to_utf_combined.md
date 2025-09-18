# pg_local_to_utf_combined

## Location
src/include/mb/pg_wchar.h: 508 - 522

## Overview
pg_local_to_utf_combined is a structure that defines a mapping entry for converting local encoding combined characters to their corresponding UTF-8 representation.

## Definition


## Detailed Description
The pg_local_to_utf_combined structure is used in PostgreSQL's character encoding conversion system to handle special cases where a single character in a local encoding corresponds to a combination of two UTF-8 characters. This structure is particularly important for handling combined characters, accented characters, or other complex character compositions where a one-to-many mapping is required during encoding conversion. The structure stores the original local encoding value and the two UTF-8 code points that represent the equivalent character combination in UTF-8.

## Parameters / Member Variables
- : The character code in the local (source) encoding that needs to be converted
- : The first UTF-8 character code in the combined character representation
- : The second UTF-8 character code in the combined character representation

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure definition)
- Called from (representative examples):
  - compare4 (for sorting/searching in conversion tables)
  - LocalToUtf (main conversion function that uses these mappings)

## Notes and Other Information
- This structure is specifically designed for local code to UTF-8 conversion scenarios
- Used in conversion tables that are typically sorted by the  field for binary search operations
- The two UTF-8 codes (utf1, utf2) represent the decomposed form of a combined character
- Essential for handling encoding conversions where character composition differs between encodings
- Part of PostgreSQL's comprehensive multibyte character conversion infrastructure
- Used in arrays that map specific local encoding characters to their UTF-8 equivalents
- The compare4 function suggests these structures are used in sorted arrays for efficient lookup
- Particularly important for encodings that use precomposed characters differently than UTF-8