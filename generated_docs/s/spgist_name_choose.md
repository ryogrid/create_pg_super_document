# spgist_name_choose

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:124-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L124-L265)

## Overview
The choose function for SP-GiST name operator class that determines how to navigate or modify the index tree when inserting a new name value.

## Definition

```c
structedValue;
```
## Detailed Description
This function implements the core logic for the SP-GiST choose operation on PostgreSQL 'name' data types. It determines how to handle a new value being inserted into the index by analyzing the relationship between the incoming value and the current node's prefix and labels. The function can return one of three results: match an existing node, add a new node, or split the current tuple.

The function handles complex scenarios including:
- Prefix matching and common prefix calculation
- Node splitting when prefixes don't fully match
- Navigation to existing child nodes when character labels match
- Addition of new nodes for previously unseen character values
- Special handling for 'allTheSame' nodes where all values are identical

The algorithm ensures efficient tree traversal while maintaining the SP-GiST invariants for text-based search operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `in` (spgChooseIn*): Input parameters including datum, current level, prefix info, and node labels
  - `out` (spgChooseOut*): Output structure specifying the chosen action and parameters

## Dependencies
- Functions called/Symbols referenced:
  - [spgChooseIn](spgChooseIn.md), spgChooseOut (SP-GiST data structures)
  - Name, DatumGetName (PostgreSQL name type handling)
  - DatumGetTextPP (text datum extraction)
  - [commonPrefix](../c/commonPrefix.md) (prefix length calculation)
  - [formTextDatum](../f/formTextDatum.md) (text datum creation)
  - [searchChar](searchChar.md) (binary search in node labels)
  - [Int16GetDatum](../I/Int16GetDatum.md) (int16 to datum conversion)
  - spgSplitTuple, spgMatchNode, spgAddNode (SP-GiST result types)
  - PG_RETURN_VOID (PostgreSQL return macro)
- Called from (representative examples):
  - SP-GiST core system during index operations

## Notes and Other Information
- Located in src/test/modules/spgist_name_ops/spgist_name_ops.c:124-265
- Part of a test module demonstrating SP-GiST operator class implementation for name types
- Implements sophisticated tree navigation logic with multiple branching scenarios
- Handles both prefix-based and character-based tree organization
- Critical for maintaining index efficiency during insertions and updates
- Uses -1 as a special nodeChar value to represent end-of-string conditions
- Uses -2 as a dummy node label in allTheSame splitting scenarios
- The function's complexity reflects the need to handle all possible tree modification scenarios

## Simplified Source

```c
Datum spgist_name_choose(PG_FUNCTION_ARGS) {
    spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
    spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
    Name inName = DatumGetName(in->datum);
    char *inStr = NameStr(*inName);
    int inSize = strlen(inStr);
    char *prefixStr = NULL;
    int prefixSize = 0;
    int commonLen = 0;
    int16 nodeChar = 0;
    int i = 0;

    // Handle existing prefix
    if (in->hasPrefix) {
        text *prefixText = DatumGetTextPP(in->prefixDatum);
        prefixStr = VARDATA_ANY(prefixText);
        prefixSize = VARSIZE_ANY_EXHDR(prefixText);

        commonLen = commonPrefix(inStr + in->level, prefixStr,
                                inSize - in->level, prefixSize);

        if (commonLen == prefixSize) {
            // Full prefix match: set character after prefix
            if (inSize - in->level > commonLen)
                nodeChar = *(unsigned char *) (inStr + in->level + commonLen);
            else
                nodeChar = -1;
        } else {
            // Partial prefix match: split tuple
            out->resultType = spgSplitTuple;

            if (commonLen == 0) {
                out->result.splitTuple.prefixHasPrefix = false;
            } else {
                out->result.splitTuple.prefixHasPrefix = true;
                out->result.splitTuple.prefixPrefixDatum =
                    formTextDatum(prefixStr, commonLen);
            }

            out->result.splitTuple.prefixNNodes = 1;
            out->result.splitTuple.prefixNodeLabels = (Datum *) palloc(sizeof(Datum));
            out->result.splitTuple.prefixNodeLabels[0] =
                Int16GetDatum(*(unsigned char *) (prefixStr + commonLen));

            out->result.splitTuple.childNodeN = 0;

            if (prefixSize - commonLen == 1) {
                out->result.splitTuple.postfixHasPrefix = false;
            } else {
                out->result.splitTuple.postfixHasPrefix = true;
                out->result.splitTuple.postfixPrefixDatum =
                    formTextDatum(prefixStr + commonLen + 1,
                                 prefixSize - commonLen - 1);
            }

            PG_RETURN_VOID();
        }
    } else if (inSize > in->level) {
        nodeChar = *(unsigned char *) (inStr + in->level);
    } else {
        nodeChar = -1;
    }

    // Search for nodeChar in existing labels
    if (searchChar(in->nodeLabels, in->nNodes, nodeChar, &i)) {
        // Descend to existing node
        int levelAdd;

        out->resultType = spgMatchNode;
        out->result.matchNode.nodeN = i;
        levelAdd = commonLen;
        if (nodeChar >= 0)
            levelAdd++;
        out->result.matchNode.levelAdd = levelAdd;
        if (inSize - in->level - levelAdd > 0)
            out->result.matchNode.restDatum =
                formTextDatum(inStr + in->level + levelAdd,
                             inSize - in->level - levelAdd);
        else
            out->result.matchNode.restDatum = formTextDatum(NULL, 0);
    } else if (in->allTheSame) {
        // Split tuple for allTheSame case
        out->resultType = spgSplitTuple;
        out->result.splitTuple.prefixHasPrefix = in->hasPrefix;
        out->result.splitTuple.prefixPrefixDatum = in->prefixDatum;
        out->result.splitTuple.prefixNNodes = 1;
        out->result.splitTuple.prefixNodeLabels = (Datum *) palloc(sizeof(Datum));
        out->result.splitTuple.prefixNodeLabels[0] = Int16GetDatum(-2);
        out->result.splitTuple.childNodeN = 0;
        out->result.splitTuple.postfixHasPrefix = false;
    } else {
        // Add new node for new character
        out->resultType = spgAddNode;
        out->result.addNode.nodeLabel = Int16GetDatum(nodeChar);
        out->result.addNode.nodeN = i;
    }

    PG_RETURN_VOID();
}
```