# NormalizeSubWord

## Location
[src/backend/tsearch/spell.c:2176-2284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2176-L2284)

## Overview
Generates all possible normalized forms of a word by systematically applying prefix and suffix transformations from the affix trees.

## Definition
```c
static char **NormalizeSubWord(IspellDict *Conf, char *word, int flag)
```

## Detailed Description
NormalizeSubWord is the core function for word normalization in PostgreSQL's spell checking system. It systematically explores all possible combinations of prefixes and suffixes to generate valid normalized forms of an input word. The function operates in three main phases:

1. **Base word check**: First checks if the word itself is already in normal form
2. **Prefix-only processing**: Tries all possible prefixes to find valid base forms
3. **Suffix-then-prefix processing**: Tries suffixes first, then applies prefixes to the suffix-transformed words

The function handles cross-product affixes (combinations of prefix and suffix) and validates each transformation against the dictionary. It maintains an array of unique results and prevents duplicates through the addToResult helper function.

## Parameters / Member Variables
- `Conf`: IspellDict configuration containing affix trees and dictionary
- `word`: Input word to normalize
- `flag`: Compound word flags indicating the word's position context

## Dependencies
- Functions called/Symbols referenced:
  - [FindAffixes](../F/FindAffixes.md) (called 3 times for prefix/suffix tree traversal)
  - [CheckAffix](../C/CheckAffix.md) (called 3 times for affix validation and transformation)
  - [addToResult](../a/addToResult.md) (called 3 times for result collection)
  - [FindWord](../F/FindWord.md) (called 4 times for dictionary validation)
  - strlen, pstrdup, palloc, pfree (utility functions)
- Called from (representative examples):
  - [SplitToVariants](../S/SplitToVariants.md) (at line 2427)
  - [NINormalizeWord](NINormalizeWord.md) (at lines 2547, 2572)

## Notes and Other Information
- Returns NULL if no valid normalized forms are found or if word exceeds MAXNORMLEN (256 characters)
- Allocates memory for up to MAX_NORM (1024) result forms
- Handles FF_CROSSPRODUCT flag for valid prefix-suffix combinations
- Uses separate buffers (newword, pnewword) for different transformation stages
- Part of PostgreSQL's text search spell checking functionality
- The baselen parameter tracks word boundaries when processing compound affixes

## Simplified Source

```c
static char **
NormalizeSubWord(IspellDict *Conf, char *word, int flag)
{
    if (strlen(word) > MAXNORMLEN)
        return NULL;

    char **forms = (char **) palloc(MAX_NORM * sizeof(char *));
    char **cur = forms;
    *cur = NULL;

    char newword[2 * MAXNORMLEN] = "";
    char pnewword[2 * MAXNORMLEN] = "";

    // Check if word itself is already normalized
    if (FindWord(Conf, word, VoidString, flag)) {
        *cur = pstrdup(word);
        cur++;
        *cur = NULL;
    }

    // Try prefix-only transformations
    AffixNode *pnode = Conf->Prefix;
    int plevel = 0;
    while (pnode) {
        AffixNodeData *prefix = FindAffixes(pnode, word, strlen(word), &plevel, FF_PREFIX);
        if (!prefix) break;

        for (int j = 0; j < prefix->naff; j++) {
            if (CheckAffix(word, strlen(word), prefix->aff[j], flag, newword, NULL)) {
                if (FindWord(Conf, newword, prefix->aff[j]->flag, flag))
                    cur += addToResult(forms, cur, newword);
            }
        }
        pnode = prefix->node;
    }

    // Try suffix transformations, then prefixes on results
    AffixNode *snode = Conf->Suffix;
    int slevel = 0;
    while (snode) {
        int baselen = 0;
        AffixNodeData *suffix = FindAffixes(snode, word, strlen(word), &slevel, FF_SUFFIX);
        if (!suffix) break;

        for (int i = 0; i < suffix->naff; i++) {
            if (CheckAffix(word, strlen(word), suffix->aff[i], flag, newword, &baselen)) {
                // Try suffix-only result
                if (FindWord(Conf, newword, suffix->aff[i]->flag, flag))
                    cur += addToResult(forms, cur, newword);

                // Try prefix+suffix combinations
                pnode = Conf->Prefix;
                plevel = 0;
                while (pnode) {
                    AffixNodeData *prefix = FindAffixes(pnode, newword, strlen(newword), &plevel, FF_PREFIX);
                    if (!prefix) break;

                    for (int j = 0; j < prefix->naff; j++) {
                        if (CheckAffix(newword, strlen(newword), prefix->aff[j], flag, pnewword, &baselen)) {
                            char *ff = (prefix->aff[j]->flagflags & suffix->aff[i]->flagflags & FF_CROSSPRODUCT) ?
                                VoidString : prefix->aff[j]->flag;

                            if (FindWord(Conf, pnewword, ff, flag))
                                cur += addToResult(forms, cur, pnewword);
                        }
                    }
                    pnode = prefix->node;
                }
            }
        }
        snode = suffix->node;
    }

    if (cur == forms) {
        pfree(forms);
        return NULL;
    }
    return forms;
}
```