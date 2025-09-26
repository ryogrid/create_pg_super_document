# PrepareSortSupportFromGistIndexRel

## Location
[src/backend/utils/sort/sortsupport.c:188-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sortsupport.c#L188-L210)

## Overview
Initializes a SortSupportData structure for sorting operations during GiST index builds by setting up the appropriate comparison function from the index's operator family.

## Definition
```c
void PrepareSortSupportFromGistIndexRel(Relation indexRel, SortSupport ssup)
```

## Detailed Description
This function configures a SortSupport structure for use with GiST (Generalized Search Tree) indexes by extracting sort support information from the index relation. It validates that the relation is indeed a GiST index, sets the reverse sort flag to false (GiST indexes always sort in ascending order during build), and locates the appropriate sort support function from the operator family.

The function performs operator family lookups to find the GIST_SORTSUPPORT_PROC function, which provides optimized comparison routines for the specific data type being indexed. This is simpler than B-tree indexes since GiST doesn't support legacy comparison functions.

Once the sort support function is found, it's called to initialize the SortSupport structure with data-type-specific comparison logic, enabling efficient sorting during index construction.

## Parameters / Member Variables
- `indexRel`: The GiST index relation containing operator family and type information
- `ssup`: Pointer to SortSupportData structure to be initialized (caller must have pre-filled ssup_cxt, ssup_attno, ssup_collation, and ssup_nulls_first)

## Dependencies
- Functions called/Symbols referenced:
  - get_opfamily_proc
  - OidFunctionCall1
  - GIST_SORTSUPPORT_PROC
  - SortSupport
- Called from (representative examples):
  - tuplesort_begin_index_gist
  - ApplySortAbbrevFullComparator

## Notes and Other Information
- The caller must zero the SortSupportData structure and pre-populate ssup_cxt, ssup_attno, ssup_collation, and ssup_nulls_first before calling this function
- The function sets ssup_reverse to false since GiST indexes always use ascending sort order during construction
- The function will error if the relation is not a GiST index or if the required sort support function is missing from the operator family
- This is specifically designed for GiST index builds and is simpler than B-tree equivalents since it doesn't need to handle legacy comparison functions
- Location: src/backend/utils/sort/sortsupport.c:188-210