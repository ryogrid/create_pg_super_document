# find_wordentry

## Location
[src/backend/utils/adt/tsrank.c:86-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L86-L134)

## Overview
Searches for a specific query operand within a TSVector using binary search, returning the corresponding WordEntry and count of matching items.

## Definition


## Detailed Description
The  function performs a binary search through the WordEntry array of a TSVector to locate entries that match a given QueryOperand from a TSQuery. It implements an efficient search algorithm that can handle both exact matches and prefix matches depending on the item's prefix flag.

The function uses a standard binary search approach with loop invariant "StopLow <= item < StopHigh" to efficiently locate matching entries. For prefix searches (when item->prefix is true), it extends the search to find all consecutive matching entries, counting them in the nitem parameter.

The function returns a pointer to the first matching WordEntry, or NULL if no matches are found. The nitem parameter is set to indicate how many consecutive entries match the query operand.

## Parameters / Member Variables
- : The TSVector to search within
- : The TSQuery containing the search operand
- : The QueryOperand to search for
- : Output parameter that receives the count of matching items

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to get array pointer from TSVector)
  -  (macro to get string pointer from TSVector)  
  -  (function to compare WordEntry with QueryOperand)
  -  (macro to get operand from TSQuery)
  -  (structure representing a lexeme entry)
  -  (text search vector type)
  -  (text search query type)
  -  (query operand structure)
- Called from (representative examples):
  -  (src/backend/utils/adt/tsrank.c:237)
  -  (src/backend/utils/adt/tsrank.c:309)
  -  (src/backend/utils/adt/tsrank.c:756)

## Notes and Other Information
- This is a static function, accessible only within tsrank.c
- Uses binary search for O(log n) time complexity when searching through WordEntry arrays
- Supports both exact matching and prefix matching based on the QueryOperand's prefix flag
- For prefix matches, the function continues searching linearly after the binary search to count all matching entries
- Critical component of PostgreSQL's text search ranking system, used to locate query terms within documents
- The function maintains proper bounds checking and handles edge cases where no matches are found
- Returns NULL when no matches are found, with nitem set to 0