# compileTheLexeme

## Location
[src/backend/tsearch/dict_thesaurus.c:391-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L391-L501)

## Overview
Processes and compiles raw lexeme entries in a thesaurus dictionary by normalizing them through a subdictionary, sorting them, and removing duplicates.

## Definition

```c
static void
compileTheLexeme(DictThesaurus *d)
```
## Detailed Description
This function performs the critical compilation phase of thesaurus dictionary initialization. It takes raw lexeme entries and transforms them into a normalized, sorted, and deduplicated array suitable for efficient runtime lookups.

The compilation process involves several key steps:
1. **Normalization**: Each lexeme is processed through a subdictionary to convert it to canonical form(s)
2. **Special handling**: Stop word markers ("?") are handled specially without subdictionary processing  
3. **Variant processing**: Handles multiple lexeme variants returned by the subdictionary
4. **Memory management**: Replaces the original lexeme array with the compiled version
5. **Sorting**: Uses qsort with cmpTheLexeme to establish lexicographic ordering
6. **Deduplication**: Removes duplicate entries while preserving all associated LexemeInfo chains

The function ensures robust error handling for unrecognized words and stop words, providing clear diagnostic messages with rule numbers to aid in thesaurus configuration debugging.

## Parameters / Member Variables
- `d`: Pointer to the DictThesaurus structure containing the raw lexeme data to be compiled

## Dependencies
- Functions called/Symbols referenced:
  - [addCompiledLexeme](../a/addCompiledLexeme.md) (adds normalized lexemes to compiled array)
  - FunctionCall4 (calls subdictionary lexize function)
  - qsort (sorts the compiled lexeme array)
  - [cmpTheLexeme](cmpTheLexeme.md) (comparison function for sorting)
  - [cmpLexeme](cmpLexeme.md) (comparison function for deduplication)
  - [cmpLexemeInfo](cmpLexemeInfo.md) (comparison function for LexemeInfo entries)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - [TheLexeme](../T/TheLexeme.md), TSLexeme, DictThesaurus (structure types)
- Called from (representative examples):
  - [thesaurus_init](../t/thesaurus_init.md)

## Notes and Other Information
- Handles both regular lexemes and stop word markers ("?") appropriately
- Processes multiple lexeme variants returned by subdictionaries (morphological analysis)
- Implements efficient deduplication by merging LexemeInfo chains for identical lexemes
- Error reporting includes rule numbers to help users debug thesaurus configuration files
- Memory efficient: replaces original array in-place and uses repalloc to resize to exact requirements
- Critical for thesaurus dictionary performance as it establishes the sorted structure needed for binary search operations
- The final sorted array enables O(log n) lookup times during phrase matching operations

## Simplified Source

```c
static void
compileTheLexeme(DictThesaurus *d)
{
    int i, nnw = 0, tnm = 16;
    TheLexeme *newwrds = (TheLexeme *) palloc(sizeof(TheLexeme) * tnm);
    TheLexeme *ptrwrds;

    // Process each raw lexeme through subdictionary
    for (i = 0; i < d->nwrds; i++)
    {
        TSLexeme *ptr;

        if (strcmp(d->wrds[i].lexeme, "?") == 0)
        {
            // Handle stop word marker specially
            newwrds = addCompiledLexeme(newwrds, &nnw, &tnm, NULL, d->wrds[i].entries, 0);
        }
        else
        {
            // Normalize lexeme through subdictionary
            ptr = (TSLexeme *) DatumGetPointer(FunctionCall4(&(d->subdict->lexize),
                    PointerGetDatum(d->subdict->dictData),
                    PointerGetDatum(d->wrds[i].lexeme),
                    Int32GetDatum(strlen(d->wrds[i].lexeme)),
                    PointerGetDatum(NULL)));

            if (!ptr)
                ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("thesaurus sample word \"%s\" isn't recognized by subdictionary (rule %d)",
                               d->wrds[i].lexeme, d->wrds[i].entries->idsubst + 1)));
            else if (!(ptr->lexeme))
                ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("thesaurus sample word \"%s\" is a stop word (rule %d)",
                               d->wrds[i].lexeme, d->wrds[i].entries->idsubst + 1),
                        errhint("Use \"?\" to represent a stop word within a sample phrase.")));
            else
            {
                // Process all variants returned by subdictionary
                while (ptr->lexeme)
                {
                    TSLexeme *remptr = ptr + 1;
                    int tnvar = 1;
                    int curvar = ptr->nvariant;

                    // Count words in this variant
                    while (remptr->lexeme)
                    {
                        if (remptr->nvariant != (remptr - 1)->nvariant)
                            break;
                        tnvar++;
                        remptr++;
                    }

                    // Add all lexemes in this variant
                    remptr = ptr;
                    while (remptr->lexeme && remptr->nvariant == curvar)
                    {
                        newwrds = addCompiledLexeme(newwrds, &nnw, &tnm, remptr, d->wrds[i].entries, tnvar);
                        remptr++;
                    }

                    ptr = remptr;
                }
            }
        }

        // Free original lexeme data
        pfree(d->wrds[i].lexeme);
        pfree(d->wrds[i].entries);
    }

    // Replace original array with compiled version
    if (d->wrds)
        pfree(d->wrds);
    d->wrds = newwrds;
    d->nwrds = nnw;
    d->ntwrds = tnm;

    // Sort and deduplicate
    if (d->nwrds > 1)
    {
        qsort(d->wrds, d->nwrds, sizeof(TheLexeme), cmpTheLexeme);

        // Remove duplicates, merging LexemeInfo chains
        newwrds = d->wrds;
        ptrwrds = d->wrds + 1;
        while (ptrwrds - d->wrds < d->nwrds)
        {
            if (cmpLexeme(ptrwrds, newwrds) == 0)
            {
                // Same lexeme - merge or discard
                if (cmpLexemeInfo(ptrwrds->entries, newwrds->entries))
                {
                    ptrwrds->entries->nextentry = newwrds->entries;
                    newwrds->entries = ptrwrds->entries;
                }
                else
                    pfree(ptrwrds->entries);

                if (ptrwrds->lexeme)
                    pfree(ptrwrds->lexeme);
            }
            else
            {
                // Different lexeme - keep it
                newwrds++;
                *newwrds = *ptrwrds;
            }
            ptrwrds++;
        }

        // Resize to actual number of unique entries
        d->nwrds = newwrds - d->wrds + 1;
        d->wrds = (TheLexeme *) repalloc(d->wrds, sizeof(TheLexeme) * d->nwrds);
    }
}
```