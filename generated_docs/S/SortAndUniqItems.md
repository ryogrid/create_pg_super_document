# SortAndUniqItems

## Location
src/backend/utils/adt/tsrank.c: 154 - 199

## Overview
Extracts, sorts, and deduplicates all operands from a TSQuery, returning an array of unique QueryOperand pointers in lexicographical order.

## Definition


## Detailed Description
The  function processes a TSQuery structure to extract all query operands (QI_VAL items), sort them lexicographically, and remove duplicates. It performs a complete preprocessing step that transforms a potentially complex query tree into a clean, sorted array of unique operands.

The function operates in several phases:
1. **Collection**: Traverses the query structure and collects all QueryOperand items into an array
2. **Sorting**: Uses  with  to sort operands by their string content
3. **Deduplication**: Removes consecutive duplicate entries by comparing adjacent sorted elements

This preprocessing is essential for efficient query processing in text search ranking algorithms, as it eliminates redundant work and provides a predictable order for operand processing.

## Parameters / Member Variables
- : The TSQuery structure to process
- : Input/output parameter - initially contains the maximum number of items, returns the actual count of unique operands

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