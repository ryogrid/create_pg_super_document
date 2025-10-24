# findVariant

## Location
[src/backend/tsearch/dict_thesaurus.c:696-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L696-L752)

## Overview
Finds matching lexeme variants by coordinating substitution IDs across multiple input lexeme arrays and building a linked list of compatible variants.

## Definition

```c
static LexemeInfo *
findVariant(LexemeInfo *in, LexemeInfo *stored, uint16 curpos, LexemeInfo **newin, int newn)
```
## Detailed Description
The  function implements a complex algorithm to find lexeme variants that match specific criteria for thesaurus substitution. It processes arrays of  pointers () to find entries with matching substitution IDs, positions, and variant counts. The function coordinates across multiple lexeme lists to ensure all input words have compatible substitution patterns. It builds a linked list of matching variants by linking them through the  field and returns the head of this list.

## Parameters / Member Variables
- `*in`: Input linked list of lexeme variants to extend (may be NULL)
- `*stored`: Previously stored lexeme information to validate against using
- `curpos`: Current position within the substitution pattern being processed
- `**newin`: Array of pointers to  structures representing input lexeme lists
- `newn`: Number of elements in the  array
## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for lexeme information and variant chaining)
  -  (validates substitution ID compatibility)
  -  (structure type referenced at end)
- Called from:
  -  (at src/backend/tsearch/dict_thesaurus.c:848)
  -  (at src/backend/tsearch/dict_thesaurus.c:855)

## Notes and Other Information
- Uses a complex nested loop structure to coordinate across multiple input lexeme arrays
- Advances through  chains in the  arrays to find matching substitution patterns
- Only adds variants that match position and variant count requirements ()
- Validates substitution compatibility using  for both stored and input lexemes
- This is a static function, only used internally within the thesaurus dictionary module
- The algorithm ensures that all input words participate in the same substitution rule

## Simplified Source

```c
static LexemeInfo *
findVariant(LexemeInfo *in, LexemeInfo *stored, uint16 curpos, LexemeInfo **newin, int newn)
{
    for (;;)
    {
        // Find minimum substitution ID across all input arrays
        LexemeInfo *ptr = newin[0];

        for (int i = 0; i < newn; i++)
        {
            // Advance each array to find matching substitution IDs
            while (newin[i] && newin[i]->idsubst < ptr->idsubst)
                newin[i] = newin[i]->nextentry;

            if (newin[i] == NULL)
                return in;  // End of any array means we're done

            // Update pointer to minimum ID found so far
            if (newin[i]->idsubst > ptr->idsubst)
            {
                ptr = newin[i];
                i = -1;  // Restart comparison
                continue;
            }

            // Look for exact match with current position and variant count
            while (newin[i]->idsubst == ptr->idsubst)
            {
                if (newin[i]->posinsubst == curpos && newin[i]->tnvariant == newn)
                {
                    ptr = newin[i];
                    break;
                }
                newin[i] = newin[i]->nextentry;
                if (newin[i] == NULL)
                    return in;
            }
        }

        // If we found a valid variant, add it to result list
        if (matchIdSubst(stored, ptr->idsubst) &&
            (in == NULL || !matchIdSubst(in, ptr->idsubst)))
        {
            ptr->nextvariant = in;
            in = ptr;
        }

        // Advance all arrays to continue search
        for (int i = 0; i < newn; i++)
            newin[i] = newin[i]->nextentry;
    }
}
```