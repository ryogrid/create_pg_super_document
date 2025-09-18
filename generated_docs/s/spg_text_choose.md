# spg_text_choose

## Location
src/backend/access/spgist/spgtextproc.c: 184 - 323

## Overview
The spg_text_choose function is the core SP-GiST choose method for text data types that determines how to navigate or modify the index tree structure when inserting or searching for text values.

## Definition
```c
Datum spg_text_choose(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SP-GiST choose method for text indexing, which is called during index traversal to determine the appropriate path through the tree. The function handles several complex scenarios:

**Prefix Matching**: When the current node has a prefix, it compares the input text with the stored prefix using commonPrefix() to determine if they match partially or completely.

**Tree Navigation**: For exact prefix matches, it extracts the next character after the prefix and uses searchChar() to locate the appropriate child node to descend to.

**Tuple Splitting**: When the input doesn't match the existing prefix, it splits the current tuple, creating a new tree structure that accommodates both the existing data and the new input.

**Node Addition**: When encountering a character that doesn't exist in the current node's label array, it adds a new node for that character.

**Special Handling**: Manages edge cases like empty strings (nodeChar = -1) and the allTheSame condition where normal node addition isn't possible.

The function's output determines whether to match an existing node, split a tuple, or add a new node, directing the SP-GiST core on how to proceed with the operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `in` (spgChooseIn): Input parameters including the datum to insert, current level, prefix information, and node labels
  - `out` (spgChooseOut): Output structure that specifies the action to take and associated parameters

## Dependencies
- Functions called/Symbols referenced:
  - spgChooseIn, spgChooseOut (structure types)
  - DatumGetTextPP (macro)
  - VARDATA_ANY, VARSIZE_ANY_EXHDR (macros)
  - commonPrefix (helper function)
  - searchChar (helper function)
  - formTextDatum (helper function)
  - Int16GetDatum (macro)
  - spgSplitTuple, spgMatchNode, spgAddNode (constants)
  - palloc (memory allocation)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - No direct callers found (typically called by SP-GiST core during index operations)

## Notes and Other Information
- This is a PostgreSQL C function following the PG_FUNCTION_ARGS convention for SP-GiST operator class methods
- The function handles three main result types: spgMatchNode (navigate to existing child), spgSplitTuple (restructure tree), and spgAddNode (create new child)
- Prefix handling enables efficient storage and traversal by factoring out common leading substrings
- The allTheSame condition occurs when all values in a node are identical, requiring special tuple splitting logic
- Character codes are stored as int16 values, with -1 representing end-of-string and -2 used as a special dummy label
- Memory allocation uses PostgreSQL's palloc system for proper memory context management
- The levelAdd mechanism tracks how many characters have been processed at each tree level