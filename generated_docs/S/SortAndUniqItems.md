# SortAndUniqItems

## Location
[src/backend/utils/adt/tsrank.c:154-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L154-L199)

## Overview
Extracts, sorts, and deduplicates all operands from a TSQuery, returning an array of unique QueryOperand pointers in lexicographical order.

## Definition

```c
static QueryOperand **
SortAndUniqItems(TSQuery q, int *size)
```
## Detailed Description
The  function processes a TSQuery structure to extract all query operands (QI_VAL items), sort them lexicographically, and remove duplicates. It performs a complete preprocessing step that transforms a potentially complex query tree into a clean, sorted array of unique operands.

The function operates in several phases:
1. **Collection**: Traverses the query structure and collects all QueryOperand items into an array
2. **Sorting**: Uses  with  to sort operands by their string content
3. **Deduplication**: Removes consecutive duplicate entries by comparing adjacent sorted elements

This preprocessing is essential for efficient query processing in text search ranking algorithms, as it eliminates redundant work and provides a predictable order for operand processing.

## Parameters / Member Variables
- `q`: The TSQuery structure to process
- `*size`: Input/output parameter - initially contains the maximum number of items, returns the actual count of unique operands
## Dependencies
- Functions called/Symbols referenced:
  -  (macro to get operand string from TSQuery)
  -  (macro to get query items from TSQuery)
  -  (PostgreSQL memory allocation function)
  -  (PostgreSQL's three-parameter qsort function)
  -  (comparison function for sorting)
  -  (query tree node structure)
  -  (query operand structure)
  -  (text search query type)
  -  (constant indicating value/operand query item)
- Called from (representative examples):
  -  (src/backend/utils/adt/tsrank.c:221)
  -  (src/backend/utils/adt/tsrank.c:301)

## Notes and Other Information
- This is a static function, accessible only within tsrank.c
- Returns pointers to the original QueryOperand structures within the query, not copies
- Handles edge cases where queries have fewer than 2 operands (no sorting/deduplication needed)
- The size parameter is both input and output - it starts with the total item count and ends with the unique operand count
- Memory is allocated using , which integrates with PostgreSQL's memory management system
- Critical preprocessing step that enables efficient text search ranking by eliminating redundant query terms
- The deduplication relies on the sorted order to identify and remove consecutive duplicates efficiently
- Essential component for optimizing complex queries that may contain repeated terms

## Simplified Source

```c
static QueryOperand **
SortAndUniqItems(TSQuery q, int *size)
{
    char *operand = GETOPERAND(q);
    QueryItem *item = GETQUERY(q);
    QueryOperand **res, **ptr, **prevptr;

    // Allocate array for operand pointers
    ptr = res = (QueryOperand **) palloc(sizeof(QueryOperand *) * *size);

    // Collect all operands from query tree
    while ((*size)--)
    {
        if (item->type == QI_VAL)
        {
            *ptr = (QueryOperand *) item;
            ptr++;
        }
        item++;
    }

    *size = ptr - res;
    if (*size < 2)
        return res;

    // Sort operands by string content
    qsort_arg(res, *size, sizeof(QueryOperand *), compareQueryOperand, operand);

    // Remove consecutive duplicates
    ptr = res + 1;
    prevptr = res;
    while (ptr - res < *size)
    {
        if (compareQueryOperand((void *) ptr, (void *) prevptr, (void *) operand) != 0)
        {
            prevptr++;
            *prevptr = *ptr;
        }
        ptr++;
    }

    *size = prevptr + 1 - res;
    return res;
}
```

This simplified version shows the three-phase process: 1) collect all query operands from the tree, 2) sort them lexicographically, and 3) remove duplicates. The result is a clean array of unique operands ready for efficient processing.