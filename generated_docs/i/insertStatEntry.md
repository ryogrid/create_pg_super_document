# insertStatEntry

## Location
[src/backend/utils/adt/tsvector_op.c:2316-2380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2316-L2380)

## Overview
A static function that inserts or updates statistical entries for words from a TSVector into a binary search tree structure used by PostgreSQL's text search statistics system.

## Definition

```c
static void
insertStatEntry(MemoryContext persistentContext, TSVectorStat *stat, TSVector txt, uint32 off)
```
## Detailed Description
The  function is a core component of PostgreSQL's  functionality that builds statistical information about words in text search vectors. It processes a single word from a TSVector and either inserts a new entry into the statistics tree or updates an existing entry.

The function maintains a binary search tree (BST) of  nodes, where each node contains information about a specific lexeme (word), including the number of documents containing it () and the total number of occurrences (). The tree is organized for efficient word lookup and traversal.

The function first determines how many occurrences to count based on whether weight filtering is enabled. If no weight filter is specified (), it counts all positions for words with position data, or 1 for words without positions. If weight filtering is enabled, it uses  to count only positions matching the specified weight criteria.

The function then searches the BST to find if the word already exists. If found, it increments the document count and entry count. If not found, it creates a new node and inserts it into the appropriate position in the tree.

## Parameters / Member Variables
- : Memory context for allocating new StatEntry nodes that need to persist across function calls
- : Pointer to the TSVectorStat structure containing the statistics tree and configuration
- : The TSVector containing the word and position data being processed
- : Offset into the TSVector's word array identifying which word to process

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get pointer to the word entry array in TSVector
  - : Macro to get length of position data for a word entry
  - : Function to count positions matching specific weight criteria
  - : Function to compare words for BST ordering
  - : PostgreSQL memory allocation function
  - : Macro to get pointer to string data in TSVector
  - : Size constant for StatEntry header
  - : Structure representing word entries in TSVector
  - : Structure representing nodes in the statistics tree
  - : Structure containing statistics tree and metadata
- Called from (representative examples):
  - : Calls this function recursively for all words in TSVectors
  - : Main ts_stat aggregation function that processes TSVectors

## Notes and Other Information
- This is a static function, accessible only within the same source file
- Maintains the BST property by using  for node comparison during insertion
- Tracks maximum tree depth in  for performance monitoring
- Uses persistent memory context to ensure StatEntry nodes survive across function calls
- Handles both positioned and non-positioned words appropriately
- Returns early if no occurrences need to be counted (when weight filtering eliminates all positions)
- Part of PostgreSQL's full-text search statistics aggregation system
- Located in 