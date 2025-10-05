# compileTheSubstitute

## Location
[src/backend/tsearch/dict_thesaurus.c:502-595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L502-L595)

## Overview
Processes and compiles substitute phrase entries in a thesaurus dictionary by normalizing them through a subdictionary and preparing them for runtime substitution operations.

## Definition

```c
static void
compileTheSubstitute(DictThesaurus *d)
```
## Detailed Description
This function performs the compilation phase for thesaurus substitute phrases, which are the replacement text that will be returned when input phrases match thesaurus rules. It processes each substitute entry through the subdictionary to normalize the lexemes, handles special flags, and manages dynamic memory allocation for variable-length results.

The compilation process includes several key operations:
1. **Lexeme normalization**: Each substitute lexeme is processed through the subdictionary unless marked with DT_USEASIS flag
2. **Dynamic array management**: Uses repalloc to grow the result array as needed to accommodate variable numbers of lexemes returned by the subdictionary
3. **Flag handling**: Preserves special flags like DT_USEASIS to bypass subdictionary processing and TSL_ADDPOS for position information
4. **Error validation**: Ensures substitute phrases are not empty and that all lexemes are recognized by the subdictionary
5. **Memory management**: Replaces original substitute arrays with compiled versions and frees temporary storage

The function is essential for preparing efficient substitute phrase matching during thesaurus query processing.

## Parameters / Member Variables
- `d`: Pointer to the DictThesaurus structure containing the raw substitute phrase data to be compiled

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall4 (calls subdictionary lexize function)  
  - [repalloc](../r/repalloc.md) (dynamic memory reallocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pfree](../p/pfree.md) (memory deallocation)
  - TSLexeme, DictThesaurus (structure types)
  - DT_USEASIS (flag to bypass lexizing)
  - TSL_ADDPOS (flag for position information)
- Called from (representative examples):
  - [thesaurus_init](../t/thesaurus_init.md)

## Notes and Other Information
- Handles the DT_USEASIS flag to allow literal substitute text without subdictionary normalization
- Implements dynamic array growth with doubling strategy for efficiency
- Sets TSL_ADDPOS flags appropriately to maintain position information in multi-lexeme substitutes
- Provides comprehensive error reporting with rule numbers for debugging thesaurus configuration
- Critical for thesaurus performance as it pre-processes all substitute phrases during initialization
- Ensures substitute phrases are never empty and all constituent lexemes are valid
- The compiled substitute arrays enable efficient phrase substitution during query processing
- Memory-efficient approach that replaces original arrays in-place and manages variable-length results dynamically

## Simplified Source

```c
static void
compileTheSubstitute(DictThesaurus *d)
{
    int i;

    // Process each substitution rule
    for (i = 0; i < d->nsubst; i++)
    {
        TSLexeme *rem = d->subst[i].res;  // Original substitute words
        TSLexeme *outptr, *inptr;
        int n = 2;  // Initial capacity

        // Create new compiled result array
        outptr = d->subst[i].res = (TSLexeme *) palloc(sizeof(TSLexeme) * n);
        outptr->lexeme = NULL;
        inptr = rem;

        // Process each substitute word
        while (inptr && inptr->lexeme)
        {
            TSLexeme *lexized, tmplex[2];

            // Check if word should be used as-is
            if (inptr->flags & DT_USEASIS)
            {
                // Don't lexize - use as-is
                tmplex[0] = *inptr;
                tmplex[0].flags = 0;
                tmplex[1].lexeme = NULL;
                lexized = tmplex;
            }
            else
            {
                // Normalize through subdictionary
                lexized = (TSLexeme *) DatumGetPointer(FunctionCall4(&(d->subdict->lexize),
                        PointerGetDatum(d->subdict->dictData),
                        PointerGetDatum(inptr->lexeme),
                        Int32GetDatum(strlen(inptr->lexeme)),
                        PointerGetDatum(NULL)));
            }

            if (lexized && lexized->lexeme)
            {
                int toset = (lexized->lexeme && outptr != d->subst[i].res) ? (outptr - d->subst[i].res) : -1;

                // Add all lexemes returned by subdictionary
                while (lexized->lexeme)
                {
                    // Expand array if needed
                    if (outptr - d->subst[i].res + 1 >= n)
                    {
                        int diff = outptr - d->subst[i].res;
                        n *= 2;
                        d->subst[i].res = (TSLexeme *) repalloc(d->subst[i].res, sizeof(TSLexeme) * n);
                        outptr = d->subst[i].res + diff;
                    }

                    // Copy lexeme
                    *outptr = *lexized;
                    outptr->lexeme = pstrdup(lexized->lexeme);

                    outptr++;
                    lexized++;
                }

                // Set position flag if needed
                if (toset > 0)
                    d->subst[i].res[toset].flags |= TSL_ADDPOS;
            }
            else if (lexized)
            {
                ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("thesaurus substitute word \"%s\" is a stop word (rule %d)",
                               inptr->lexeme, i + 1)));
            }
            else
            {
                ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("thesaurus substitute word \"%s\" isn't recognized by subdictionary (rule %d)",
                               inptr->lexeme, i + 1)));
            }

            // Free original substitute word
            if (inptr->lexeme)
                pfree(inptr->lexeme);
            inptr++;
        }

        // Validate substitute phrase is not empty
        if (outptr == d->subst[i].res)
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("thesaurus substitute phrase is empty (rule %d)", i + 1)));

        // Set final length and clean up
        d->subst[i].reslen = outptr - d->subst[i].res;
        pfree(rem);
    }
}
```