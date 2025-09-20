# spgNodePtr

## Location
[src/backend/access/spgist/spgtextproc.c:87-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L87-L92)

## Overview
 is a struct used for sorting values during the picksplit operation in SP-GiST (Space-Partitioned Generalized Search Tree) text processing, specifically in the radix tree implementation for text indexing.

## Definition

```c
typedef struct spgNodePtr
{
	Datum		d;
	int			i;
	int16		c;
} spgNodePtr;
```
## Detailed Description
The  struct is a helper data structure used within the  function to organize and sort text values during the node splitting process in SP-GiST text indexes. When building a radix tree (compressed trie) over text data, this struct facilitates the grouping of strings by their distinguishing characters after a common prefix has been identified.

The struct serves as an intermediary representation that combines the original datum with metadata needed for the sorting and partitioning algorithm. During picksplit operations, an array of these structs is created, populated with values from the input tuples, sorted by the character field, and then used to create the output node structure.

This is part of PostgreSQL's SP-GiST access method implementation specifically tailored for text data types, enabling efficient prefix-based searching and range queries on string data.

## Parameters / Member Variables
- : The original Datum value (text string) being processed in the picksplit operation
- : The index of this tuple in the original input array, used to maintain correspondence between sorted values and their original positions
- : The distinguishing character (as int16) that appears immediately after the common prefix; used as the sort key and becomes the node label (-1 for strings that are entirely within the common prefix)

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
  - int16 (PostgreSQL type alias)

- Called from (representative examples):
  - [cmpNodePtr](../c/cmpNodePtr.md) (comparison function for qsort)
  - [spg_text_picksplit](spg_text_picksplit.md) (main picksplit function for text SP-GiST)

## Notes and Other Information
- This struct is used only internally within the SP-GiST text processing module and is not exposed in any public API
- The  field can contain special values: -1 indicates that the string has no characters beyond the common prefix
- The struct is dynamically allocated in arrays during picksplit operations and freed when the operation completes
- Sorting is performed using the  comparison function which compares the  field values using 
- This structure is part of PostgreSQL's implementation of radix trees for text indexing, providing efficient storage and retrieval of string data with common prefixes