# mkANode

## Location
[src/backend/tsearch/spell.c:1830-1906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1830-L1906)

## Overview
mkANode builds a prefix tree (Trie) for affix rules with non-empty replacement strings, creating an efficient hierarchical structure for affix matching during spell checking.

## Definition

```c
static AffixNode *
mkANode(IspellDict *Conf, int low, int high, int level, int type)
```
## Detailed Description
This recursive function constructs a prefix tree from a range of affix rules to enable fast pattern matching. It processes affixes level by level, grouping them by their character at the current position. The function builds a tree where:

1. **Character Counting**: First pass counts unique characters at the current level
2. **Node Creation**: Allocates an AffixNode with appropriate size for character branches  
3. **Recursive Building**: For each unique character, recursively builds child nodes for the next level
4. **Affix Collection**: Collects complete affixes (where replacement length equals current level + 1)

The resulting tree structure allows efficient traversal during affix matching, where each node represents a character position and contains both child nodes for longer patterns and completed affix rules.

## Parameters / Member Variables
- `*Conf`: Pointer to IspellDict containing affix configuration and data
- `low`: Lower index in the Conf->Affix array for processing range
- `high`: Upper index in the Conf->Affix array for processing range
- `level`: Current depth/level in the prefix tree being built
- `type`: Affix type - either FF_SUFFIX or FF_PREFIX indicating processing direction
## Dependencies
- Functions called/Symbols referenced:
  - GETCHAR (macro for character extraction)
  - tmpalloc
  - cpalloc0
  - cpalloc
  - memcpy
  - [pfree](../p/pfree.md)
  - [mkANode](mkANode.md) (recursive self-call)
- Called from (representative examples):
  - [mkANode](mkANode.md) (recursive calls)
  - [NISortAffixes](../N/NISortAffixes.md)

## Notes and Other Information
- Only processes affixes with non-empty replacement strings; empty affixes are handled by mkVoidAffix()
- Uses recursive strategy to build tree level by level
- Memory allocation uses cpalloc0 for the main node and cpalloc for affix arrays
- The ANHRDSZ constant defines the header size for AffixNode structures
- Temporary affix array is allocated with tmpalloc and freed after use
- Returns NULL if no characters are found at the current level, indicating end of tree branch

## Simplified Source

```c
static AffixNode *
mkANode(IspellDict *Conf, int low, int high, int level, int type)
{
    int nchar = 0;
    uint8 lastchar = '\0';

    // Count unique characters at current level
    for (int i = low; i < high; i++)
        if (Conf->Affix[i].replen > level &&
            lastchar != GETCHAR(Conf->Affix + i, level, type))
        {
            nchar++;
            lastchar = GETCHAR(Conf->Affix + i, level, type);
        }

    if (!nchar)
        return NULL;

    // Allocate node and initialize
    AffixNode *rs = cpalloc0(ANHRDSZ + nchar * sizeof(AffixNodeData));
    rs->length = nchar;
    AffixNodeData *data = rs->data;

    AFFIX **aff = tmpalloc(sizeof(AFFIX *) * (high - low + 1));
    int naff = 0;
    int lownew = low;

    lastchar = '\0';

    // Build tree structure level by level
    for (int i = low; i < high; i++)
        if (Conf->Affix[i].replen > level)
        {
            uint8 currentchar = GETCHAR(Conf->Affix + i, level, type);

            // New character - create branch
            if (lastchar != currentchar)
            {
                if (lastchar)
                {
                    // Recursively build next level
                    data->node = mkANode(Conf, lownew, i, level + 1, type);

                    // Store collected affixes for this branch
                    if (naff)
                    {
                        data->naff = naff;
                        data->aff = cpalloc(sizeof(AFFIX *) * naff);
                        memcpy(data->aff, aff, sizeof(AFFIX *) * naff);
                        naff = 0;
                    }
                    data++;
                    lownew = i;
                }
                lastchar = currentchar;
            }

            data->val = currentchar;

            // Affix ends at this level - collect it
            if (Conf->Affix[i].replen == level + 1)
                aff[naff++] = Conf->Affix + i;
        }

    // Handle final branch
    data->node = mkANode(Conf, lownew, high, level + 1, type);
    if (naff)
    {
        data->naff = naff;
        data->aff = cpalloc(sizeof(AFFIX *) * naff);
        memcpy(data->aff, aff, sizeof(AFFIX *) * naff);
    }

    pfree(aff);
    return rs;
}
```