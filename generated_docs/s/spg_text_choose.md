# spg_text_choose

## Location
[src/backend/access/spgist/spgtextproc.c:184-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L184-L323)

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
  - [spgChooseIn](spgChooseIn.md), spgChooseOut (structure types)
  - DatumGetTextPP (macro)
  - VARDATA_ANY, VARSIZE_ANY_EXHDR (macros)
  - [commonPrefix](../c/commonPrefix.md) (helper function)
  - [searchChar](searchChar.md) (helper function)
  - [formTextDatum](../f/formTextDatum.md) (helper function)
  - [Int16GetDatum](../I/Int16GetDatum.md) (macro)
  - spgSplitTuple, spgMatchNode, spgAddNode (constants)
  - [palloc](../p/palloc.md) (memory allocation)
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

## Simplified Source

```c
Datum spg_text_choose(PG_FUNCTION_ARGS) {
    spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
    spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
    text *inText = DatumGetTextPP(in->datum);
    char *inStr = VARDATA_ANY(inText);
    int inSize = VARSIZE_ANY_EXHDR(inText);
    int commonLen = 0;
    int16 nodeChar = 0;
    int i = 0;

    // Check for prefix match and extract node character
    if (in->hasPrefix) {
        text *prefixText = DatumGetTextPP(in->prefixDatum);
        char *prefixStr = VARDATA_ANY(prefixText);
        int prefixSize = VARSIZE_ANY_EXHDR(prefixText);

        // Find common prefix length
        commonLen = commonPrefix(inStr + in->level, prefixStr,
                                inSize - in->level, prefixSize);

        if (commonLen == prefixSize) {
            // Full prefix match - get next character
            if (inSize - in->level > commonLen)
                nodeChar = *(unsigned char *)(inStr + in->level + commonLen);
            else
                nodeChar = -1; // End of string
        } else {
            // Prefix mismatch - must split tuple
            out->resultType = spgSplitTuple;

            // Set up prefix for new upper tuple
            if (commonLen == 0) {
                out->result.splitTuple.prefixHasPrefix = false;
            } else {
                out->result.splitTuple.prefixHasPrefix = true;
                out->result.splitTuple.prefixPrefixDatum =
                    formTextDatum(prefixStr, commonLen);
            }

            // Create single node with diverging character
            out->result.splitTuple.prefixNNodes = 1;
            out->result.splitTuple.prefixNodeLabels = (Datum *) palloc(sizeof(Datum));
            out->result.splitTuple.prefixNodeLabels[0] =
                Int16GetDatum(*(unsigned char *)(prefixStr + commonLen));

            // Set up postfix for child tuple
            if (prefixSize - commonLen == 1) {
                out->result.splitTuple.postfixHasPrefix = false;
            } else {
                out->result.splitTuple.postfixHasPrefix = true;
                out->result.splitTuple.postfixPrefixDatum =
                    formTextDatum(prefixStr + commonLen + 1, prefixSize - commonLen - 1);
            }

            PG_RETURN_VOID();
        }
    } else if (inSize > in->level) {
        // No prefix - get character at current level
        nodeChar = *(unsigned char *)(inStr + in->level);
    } else {
        nodeChar = -1; // End of string
    }

    // Search for nodeChar in existing node labels
    if (searchChar(in->nodeLabels, in->nNodes, nodeChar, &i)) {
        // Match found - descend to existing node
        out->resultType = spgMatchNode;
        out->result.matchNode.nodeN = i;

        int levelAdd = commonLen;
        if (nodeChar >= 0) levelAdd++;
        out->result.matchNode.levelAdd = levelAdd;

        // Set remaining data for child level
        if (inSize - in->level - levelAdd > 0) {
            out->result.matchNode.restDatum =
                formTextDatum(inStr + in->level + levelAdd, inSize - in->level - levelAdd);
        } else {
            out->result.matchNode.restDatum = formTextDatum(NULL, 0);
        }
    } else if (in->allTheSame) {
        // Special case: can't add node, must split
        out->resultType = spgSplitTuple;
        out->result.splitTuple.prefixHasPrefix = in->hasPrefix;
        out->result.splitTuple.prefixPrefixDatum = in->prefixDatum;
        out->result.splitTuple.prefixNNodes = 1;
        out->result.splitTuple.prefixNodeLabels = (Datum *) palloc(sizeof(Datum));
        out->result.splitTuple.prefixNodeLabels[0] = Int16GetDatum(-2); // Dummy label
        out->result.splitTuple.postfixHasPrefix = false;
    } else {
        // Add new node for unseen character
        out->resultType = spgAddNode;
        out->result.addNode.nodeLabel = Int16GetDatum(nodeChar);
        out->result.addNode.nodeN = i;
    }

    PG_RETURN_VOID();
}
```