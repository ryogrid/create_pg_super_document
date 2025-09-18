# gintuple_get_attrnum

## Location
[src/backend/access/gin/ginutil.c:226-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L226-L258)

## Overview
Extracts the attribute (column) number from a stored GIN index tuple, handling both single-column and multi-column index cases.

## Definition


## Detailed Description
This function determines which column (attribute) of the original table a GIN index tuple represents. For single-column indexes, the column number is implicitly known to be the first column. For multi-column indexes, the column number is explicitly stored as the first attribute in the index tuple as an INT16 value.

The function uses the GinState structure to determine whether this is a single-column index () and acts accordingly. For multi-column indexes, it extracts the first attribute from the tuple, which always contains the column number as a 16-bit integer.

## Parameters / Member Variables
- : Pointer to the GinState structure containing index metadata
- : The IndexTuple from which to extract the attribute number

## Dependencies
- Functions called/Symbols referenced:
  -  (extract attribute from tuple)
  -  (convert Datum to 16-bit unsigned integer)
  -  (constant for first attribute position)
  -  (debugging assertions)

- Called from:
  -  (src/backend/access/gin/ginentrypage.c:254)
  -  (src/backend/access/gin/ginentrypage.c:311)
  -  (src/backend/access/gin/ginentrypage.c:382)
  -  (src/backend/access/gin/ginfast.c:733)
  -  (src/backend/access/gin/ginget.c:166, 277)
  -  (src/backend/access/gin/ginget.c:1558)
  -  (src/backend/access/gin/ginget.c:1674)
  -  (src/backend/access/gin/gininsert.c:63)
  -  (src/backend/access/gin/ginutil.c:279)
  -  (src/backend/access/gin/ginvacuum.c:542)

## Notes and Other Information
- For single-column indexes ( is true), always returns  without examining the tuple
- For multi-column indexes, the first attribute of every GIN tuple is always an INT16 containing the original column number
- The function includes assertions to verify that the extracted column number is valid (within the range of the original table's attributes)
- This is a fundamental utility function used throughout the GIN access method implementation for tuple processing
- The function assumes that multi-column GIN tuples follow the standard format where the first attribute is the column number
- Return type is , which is typically a 16-bit integer type used for attribute numbering in PostgreSQL