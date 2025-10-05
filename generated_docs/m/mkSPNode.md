# mkSPNode

## Location
[src/backend/tsearch/spell.c:1639-1720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1639-L1720)

## Overview
Recursively constructs a prefix tree (trie) structure for spell-checking, organizing dictionary words by character prefixes at each level.

## Definition
```c
static SPNode *mkSPNode(IspellDict *Conf, int low, int high, int level)
```

## Detailed Description
This function builds a prefix tree structure for efficient spell-checking by recursively partitioning the sorted dictionary words. At each level, it groups words by their character at the current position, creating SPNode structures that contain character values and either child nodes (for continued paths) or word completion information (for word endings). The function handles affix merging when multiple words share the same prefix but have different affixes, and manages compound word flags appropriately. It implements special logic for compound-only words and ensures proper flag inheritance.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing the sorted dictionary
- `low`: Lower index boundary in the Conf->Spell array for current partition
- `high`: Upper index boundary in the Conf->Spell array for current partition  
- `level`: Current depth/character position in the prefix tree

## Dependencies
- Functions called/Symbols referenced:
  - cpalloc0 (zero-initialized memory allocation)
  - [mkSPNode](mkSPNode.md) (recursive calls for child nodes)
  - [makeCompoundFlags](makeCompoundFlags.md) (extracts compound flags from affix)
  - [MergeAffix](../M/MergeAffix.md) (merges multiple affix sets)
  - SPNHDRSZ (SPNode header size constant)
  - FF_COMPOUNDONLY/FF_COMPOUNDFLAG (compound word flag constants)
- Called from (representative examples):
  - [mkSPNode](mkSPNode.md) (recursive self-calls)
  - [NISortDictionary](../N/NISortDictionary.md) (initial tree construction)

## Notes and Other Information
- Returns NULL if no characters are found at the current level
- Allocates SPNode with header plus array of SPNodeData for each unique character
- Handles word completion by setting isword flag and storing affix information
- Implements affix merging logic when multiple words end at the same node
- Manages compound word flags with special handling for FF_COMPOUNDONLY
- Automatically promotes compound-only words to compound flags when appropriate
- Uses clearCompoundOnly logic to handle conflicting compound permissions
- Tree structure enables efficient prefix-based word lookup and validation

## Simplified Source

```c
static SPNode *mkSPNode(IspellDict *Conf, int low, int high, int level) {
    int i, nchar = 0;
    char lastchar = '\0';
    SPNode *rs;
    SPNodeData *data;
    int lownew = low;

    // Count unique characters at this level
    for (i = low; i < high; i++) {
        if (Conf->Spell[i]->p.d.len > level &&
            lastchar != Conf->Spell[i]->word[level]) {
            nchar++;
            lastchar = Conf->Spell[i]->word[level];
        }
    }

    if (!nchar)
        return NULL;

    // Allocate node with space for character data
    rs = (SPNode *) cpalloc0(SPNHDRSZ + nchar * sizeof(SPNodeData));
    rs->length = nchar;
    data = rs->data;

    // Build node data for each unique character
    lastchar = '\0';
    for (i = low; i < high; i++) {
        if (Conf->Spell[i]->p.d.len > level) {
            if (lastchar != Conf->Spell[i]->word[level]) {
                if (lastchar) {
                    // Recursively build child node for previous character
                    data->node = mkSPNode(Conf, lownew, i, level + 1);
                    lownew = i;
                    data++;
                }
                lastchar = Conf->Spell[i]->word[level];
            }

            // Set character value
            data->val = ((uint8 *) (Conf->Spell[i]->word))[level];

            // Handle word completion at this level
            if (Conf->Spell[i]->p.d.len == level + 1) {
                bool clearCompoundOnly = false;

                // Handle affix merging for duplicate words
                if (data->isword && data->affix != Conf->Spell[i]->p.d.affix) {
                    // Check compound flag compatibility
                    clearCompoundOnly = (FF_COMPOUNDONLY & data->compoundflag
                                       & makeCompoundFlags(Conf, Conf->Spell[i]->p.d.affix))
                                      ? false : true;
                    data->affix = MergeAffix(Conf, data->affix, Conf->Spell[i]->p.d.affix);
                } else {
                    data->affix = Conf->Spell[i]->p.d.affix;
                }

                data->isword = 1;
                data->compoundflag = makeCompoundFlags(Conf, data->affix);

                // Auto-promote compound-only words to compound flags
                if ((data->compoundflag & FF_COMPOUNDONLY) &&
                    (data->compoundflag & FF_COMPOUNDFLAG) == 0)
                    data->compoundflag |= FF_COMPOUNDFLAG;

                // Clear compound-only if there's a conflict
                if (clearCompoundOnly)
                    data->compoundflag &= ~FF_COMPOUNDONLY;
            }
        }
    }

    // Build final child node
    data->node = mkSPNode(Conf, lownew, high, level + 1);

    return rs;
}
```