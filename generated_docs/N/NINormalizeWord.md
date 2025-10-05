# NINormalizeWord

## Location
[src/backend/tsearch/spell.c:2540-2606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2540-L2606)

## Overview
The main entry point for normalizing words using PostgreSQL's Ispell dictionary, producing all possible lexical forms including compound word variants.

## Definition
```c
TSLexeme *NINormalizeWord(IspellDict *Conf, char *word)
```

## Detailed Description
NINormalizeWord is the primary function for word normalization in PostgreSQL's Ispell-based text search dictionary. It processes input words through two main phases: first, it attempts direct normalization using NormalizeSubWord to find dictionary matches. Second, if compound word processing is enabled, it uses SplitToVariants to break the word into components and normalizes each part. The function returns a null-terminated array of TSLexeme structures containing all possible normalized forms with their associated variant numbers. This function is crucial for text search functionality as it bridges raw text input with the searchable lexical forms stored in the search index.

## Parameters / Member Variables
- `Conf`: IspellDict configuration containing dictionary rules and compound word settings
- `word`: Input word string to be normalized

## Dependencies
- Functions called/Symbols referenced:
  - [NormalizeSubWord](NormalizeSubWord.md) (direct word normalization)
  - [SplitToVariants](../S/SplitToVariants.md) (compound word splitting)
  - [addNorm](../a/addNorm.md) (adding normalized forms to result array)
  - strlen (string length calculation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [palloc](../p/palloc.md)/pfree (memory management)
- Called from (representative examples):
  - [dispell_lexize](../d/dispell_lexize.md) (at src/backend/tsearch/dict_ispell.c:125)

## Notes and Other Information
- Returns NULL if no normalization is possible
- Handles both simple words and compound words when usecompound is enabled
- Assigns incrementing variant numbers to distinguish different normalizations
- Manages memory allocation and cleanup for both successful and failed normalizations
- For compound words, processes all components and generates combinations of their normalized forms
- Critical component of PostgreSQL's full-text search infrastructure
- Used by the Ispell dictionary type in text search configurations

## Simplified Source

```c
TSLexeme *NINormalizeWord(IspellDict *Conf, char *word) {
    TSLexeme *result_list = NULL, *current = NULL;
    uint16 variant_num = 1;

    // Phase 1: Direct normalization
    char **normalizations = NormalizeSubWord(Conf, word, 0);
    if (normalizations) {
        char **ptr = normalizations;
        while (*ptr && (current - result_list) < MAX_NORM) {
            addNorm(&result_list, &current, *ptr, 0, variant_num++);
            ptr++;
        }
        pfree(normalizations);
    }

    // Phase 2: Compound word processing (if enabled)
    if (Conf->usecompound) {
        int wordlen = strlen(word);
        SplitVar *variants = SplitToVariants(Conf, NULL, NULL, word, wordlen, 0, -1);

        while (variants) {
            if (variants->nstem > 1) {
                // Normalize the last component with compound flag
                char **last_stem_forms = NormalizeSubWord(Conf,
                    variants->stem[variants->nstem - 1], FF_COMPOUNDLAST);

                if (last_stem_forms) {
                    char **form_ptr = last_stem_forms;
                    while (*form_ptr) {
                        // Add all stem components plus normalized last component
                        for (int i = 0; i < variants->nstem - 1; i++) {
                            addNorm(&result_list, &current,
                                (form_ptr == last_stem_forms) ? variants->stem[i] : pstrdup(variants->stem[i]),
                                0, variant_num);
                        }
                        addNorm(&result_list, &current, *form_ptr, 0, variant_num);
                        form_ptr++;
                        variant_num++;
                    }
                    pfree(last_stem_forms);
                }
            }

            // Cleanup current variant and move to next
            SplitVar *next = variants->next;
            // ... cleanup code for stems and variant structure
            variants = next;
        }
    }

    return result_list;
}
```