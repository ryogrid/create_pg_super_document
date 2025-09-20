# compare_text_lexemes

## Location
[src/backend/utils/adt/tsvector_op.c:442-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L442-L463)

## Overview
A static qsort comparator function that compares two text lexemes (represented as Datum values) for sorting purposes in TSVector operations.

## Definition

```c
static int
compare_text_lexemes(const void *va, const void *vb)
```
## Detailed Description
The  function serves as a comparator function for sorting arrays of text lexemes during TSVector construction. It takes two void pointers (following qsort interface requirements), dereferences them as Datum values representing PostgreSQL text objects, extracts the actual string data from these text objects, and compares them using TSVector's specialized string comparison function.

The function process:
1. Extracts Datum values from the void pointers
2. Uses  to get the actual string data from each text object
3. Uses  to determine string lengths (excluding headers)
4. Delegates to  for the actual lexicographical comparison

This enables proper lexicographical ordering of lexemes when constructing TSVectors from text arrays.

## Parameters / Member Variables
- : Pointer to the first Datum (text object) to compare (cast from void*)
- : Pointer to the second Datum (text object) to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY (macro to extract string data from varlena objects)
  - VARSIZE_ANY_EXHDR (macro to get string length excluding header)
  - [tsCompareString](../t/tsCompareString.md) (TSVector-specific string comparison function)
- Called from:
  - [array_to_tsvector](../a/array_to_tsvector.md) (used with qsort to sort lexemes during TSVector construction)

## Notes and Other Information
- Designed specifically for use with qsort when processing text arrays for TSVector creation
- Handles PostgreSQL's varlena text representation (variable-length data with headers)
- Uses TSVector's specialized comparison function which may have different rules than standard string comparison
- Critical for ensuring lexemes are properly sorted in TSVector construction
- The void pointer interface is required by qsort function signature
- Part of the array-to-TSVector conversion pipeline