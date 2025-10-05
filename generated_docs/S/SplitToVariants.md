# SplitToVariants

## Location
[src/backend/tsearch/spell.c:2374-2523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2374-L2523)

## Overview
A recursive function that splits compound words into component parts and generates all possible variants for spell checking in PostgreSQL's text search functionality.

## Definition
```c
static SplitVar *SplitToVariants(IspellDict *Conf, SPNode *snode, SplitVar *orig, char *word, int wordlen, int startpos, int minpos)
```

## Detailed Description
SplitToVariants is a complex recursive function that analyzes compound words by attempting to split them at various positions and checking if the resulting parts exist in the dictionary. It traverses the spell dictionary trie structure while tracking possible word boundaries, handling compound affixes, and generating multiple splitting variants. The function implements PostgreSQL's compound word recognition algorithm, which is essential for languages that frequently use compound words. It uses a backtracking approach to explore all possible valid splits and builds a linked list of SplitVar structures containing the different word stem combinations.

## Parameters / Member Variables
- `Conf`: IspellDict configuration containing dictionary and affix rules
- `snode`: Current node in the spell dictionary trie (NULL to start from root)
- `orig`: Original SplitVar structure to copy and extend
- `word`: Input word string to be split
- `wordlen`: Length of the input word
- `startpos`: Starting position for current split attempt
- `minpos`: Minimum position for valid word boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [CopyVar](../C/CopyVar.md) (copying SplitVar structures)
  - [CheckCompoundAffixes](../C/CheckCompoundAffixes.md) (compound affix validation)
  - [NormalizeSubWord](../N/NormalizeSubWord.md) (word normalization)
  - [AddStem](../A/AddStem.md) (adding stems to variants)
  - [palloc](../p/palloc.md)/pfree (memory management)
  - [pnstrdup](../p/pnstrdup.md) (string duplication)
- Called from (representative examples):
  - [SplitToVariants](SplitToVariants.md) (recursive calls at src/backend/tsearch/spell.c:2446, 2501)
  - [NINormalizeWord](../N/NINormalizeWord.md) (at src/backend/tsearch/spell.c:2565)

## Notes and Other Information
- Implements recursive backtracking with stack depth checking to prevent overflow
- Handles three types of compound positions: FF_COMPOUNDBEGIN, FF_COMPOUNDMIDDLE, FF_COMPOUNDLAST
- Uses a 'notprobed' array to avoid redundant checks at the same positions
- Performs binary search on trie nodes for efficient character matching
- The function can generate multiple splitting variants for the same word
- Critical for text search in languages with extensive compound word usage
- Part of PostgreSQL's Ispell-based spell checking infrastructure

## Simplified Source

```c
static SplitVar *
SplitToVariants(IspellDict *Conf, SPNode *snode, SplitVar *orig, char *word, int wordlen, int startpos, int minpos)
{
    check_stack_depth();  // Prevent stack overflow

    // Initialize tracking and result structures
    char *notprobed = (char *) palloc(wordlen);
    memset(notprobed, 1, wordlen);
    SplitVar *var = CopyVar(orig, 1);

    SPNode *node = snode ? snode : Conf->Dictionary;
    int level = snode ? minpos : startpos;

    while (level < wordlen) {
        // Check for compound affixes
        CMPDAffix *caff = Conf->CompoundAffix;
        while (level > startpos) {
            int lenaff = CheckCompoundAffixes(&caff, word + level, wordlen - level, node ? true : false);
            if (lenaff < 0) break;

            // Process found compound affix
            char buf[MAXNORMLEN];
            lenaff = level - startpos + lenaff;

            if (!notprobed[startpos + lenaff - 1] ||
                level + lenaff - 1 <= minpos ||
                lenaff >= MAXNORMLEN)
                continue;

            if (lenaff > 0)
                memcpy(buf, word + startpos, lenaff);
            buf[lenaff] = '\0';

            // Determine compound flag
            int compoundflag;
            if (level == 0)
                compoundflag = FF_COMPOUNDBEGIN;
            else if (level == wordlen - 1)
                compoundflag = FF_COMPOUNDLAST;
            else
                compoundflag = FF_COMPOUNDMIDDLE;

            // Try to normalize the compound part
            char **subres = NormalizeSubWord(Conf, buf, compoundflag);
            if (subres) {
                // Found valid compound part - create new variant
                SplitVar *new = CopyVar(var, 0);
                notprobed[startpos + lenaff - 1] = 0;

                // Add all normalized forms to the new variant
                char **sptr = subres;
                while (*sptr) {
                    AddStem(new, *sptr);
                    sptr++;
                }
                pfree(subres);

                // Continue recursively with remaining word
                SplitVar *ptr = var;
                while (ptr->next)
                    ptr = ptr->next;
                ptr->next = SplitToVariants(Conf, NULL, new, word, wordlen,
                    startpos + lenaff, startpos + lenaff);

                pfree(new->stem);
                pfree(new);
            }
        }

        if (!node) break;

        // Binary search in dictionary trie
        SPNodeData *StopLow = node->data;
        SPNodeData *StopHigh = node->data + node->length;
        SPNodeData *StopMiddle = NULL;

        while (StopLow < StopHigh) {
            StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);
            uint8 ch = ((uint8 *) word)[level];

            if (StopMiddle->val == ch)
                break;
            else if (StopMiddle->val < ch)
                StopLow = StopMiddle + 1;
            else
                StopHigh = StopMiddle;
        }

        // Process if character found in trie
        if (StopLow < StopHigh) {
            // Set compound flag based on position
            int compoundflag;
            if (startpos == 0)
                compoundflag = FF_COMPOUNDBEGIN;
            else if (level == wordlen - 1)
                compoundflag = FF_COMPOUNDLAST;
            else
                compoundflag = FF_COMPOUNDMIDDLE;

            // Check if this is a valid word boundary
            if (StopMiddle->isword &&
                (StopMiddle->compoundflag & compoundflag) &&
                notprobed[level] &&
                level > minpos) {

                if (wordlen == level + 1) {
                    // Found complete word
                    AddStem(var, pnstrdup(word + startpos, wordlen - startpos));
                    pfree(notprobed);
                    return var;
                }
                else {
                    // Continue searching for longer words
                    SplitVar *ptr = var;
                    while (ptr->next)
                        ptr = ptr->next;
                    ptr->next = SplitToVariants(Conf, node, var, word, wordlen, startpos, level);

                    level++;
                    AddStem(var, pnstrdup(word + startpos, level - startpos));
                    node = Conf->Dictionary;
                    startpos = level;
                    continue;
                }
            }
            node = StopMiddle->node;
        }
        else {
            node = NULL;
        }
        level++;
    }

    // Add remaining word part as stem
    AddStem(var, pnstrdup(word + startpos, wordlen - startpos));
    pfree(notprobed);
    return var;
}
```