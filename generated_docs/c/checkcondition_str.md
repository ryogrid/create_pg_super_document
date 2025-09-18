# checkcondition_str

## Location
src/backend/utils/adt/tsvector_op.c: 1295 - 1462

## Overview
Callback function for TS_execute that searches a tsvector for lexemes matching a query operand, handling both exact matches and prefix searches with optional position data collection for phrase matching.

## Definition


## Detailed Description
The  function is the core matching engine used by PostgreSQL's text search execution framework. It implements a binary search algorithm to locate lexemes in a tsvector that match a query operand, supporting both exact string matching and prefix matching modes.

For exact matches, the function performs a standard binary search through the sorted WordEntry array. For prefix searches, it extends the search to find all lexemes where the query term is a prefix, collecting and merging position information from multiple matching entries.

When position data is requested (for phrase queries), the function aggregates positions from all matching lexemes, sorts them, removes duplicates, and returns them in the ExecPhraseData structure. The function carefully manages memory allocation and handles the complex logic of combining results from multiple prefix matches.

## Parameters / Member Variables
- **checkval**: Void pointer to CHKVAL structure containing tsvector access data
  - Cast internally to access WordEntry array bounds and string data
- **val**: QueryOperand structure specifying the search criteria
  - Contains the operand string, length, distance offset, prefix flag, and weight restrictions
- **data**: Optional ExecPhraseData structure for position information collection
  - NULL if positions are not needed, populated with matching positions if provided

## Dependencies
- Functions called/Symbols referenced:
  - tsCompareString: Core string comparison function supporting prefix matching
  - checkclass_str: Weight checking and position filtering function
  - palloc: Allocate memory in PostgreSQL memory context
  - pfree: Free allocated memory
  - repalloc: Reallocate memory with new size
  - qsort: Standard C library sorting function
  - qunique: PostgreSQL utility to remove duplicates from sorted array
  - compareWordEntryPos: Comparison function for WordEntryPos sorting
  - TSTernaryValue constants (TS_YES, TS_NO, TS_MAYBE)
  - WordEntry: Lexeme entry structure in tsvector
  - WordEntryPos: Position and weight information structure
  - ExecPhraseData: Structure for collecting phrase matching positions

- Called from (representative examples):
  - ts_match_vq: Main tsvector-tsquery matching function

## Notes and Other Information
- Implements the TSExecuteCallback interface for the TS_execute framework
- Uses binary search for O(log n) lexeme lookup performance in sorted tsvectors
- Prefix search extends beyond exact match to find all matching prefixes
- Position aggregation uses dynamic memory allocation, starting with 256 positions and doubling as needed
- Returns TS_YES for definite match, TS_NO for no match, TS_MAYBE for uncertain cases
- Handles complex logic for combining position data from multiple prefix matches
- Memory management includes careful cleanup of temporary allocations during prefix searches  
- Position arrays are sorted and deduplicated to ensure consistent results
- Critical component in phrase query execution and proximity-based text search operations
- Supports weight-restricted searches through delegation to checkclass_str
- Optimized for the common case of exact matching while supporting the more complex prefix search scenario