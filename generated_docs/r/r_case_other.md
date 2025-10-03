# r_case_other

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c:623-652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c#L623-L652)

## Overview
The r_case_other function handles Hungarian sublative case endings ending in 'l', specifically processing 'stul', 'astul', 'ástul', 'stül', 'estül', and 'éstül' suffixes with different transformation rules.

## Definition

```c
}

static int r_case_other(struct SN_env * z)
```
## Detailed Description
This function processes sublative case endings in Hungarian that express motion onto or attachment to something ("from/off of"). The function handles six specific patterns ending in 'l':

1. 'stul' and 'stül' → complete removal (case 1)
2. 'astul' and 'estül' → replace with 'a' or 'e' respectively (case 2)  
3. 'ástul' and 'éstül' → replace with 'a' or 'e' respectively (case 3)

The function first checks that the word has at least 4 characters and ends with 'l' (ASCII 108), then uses find_among_b to match against the sublative patterns in array a_6. 

Based on the matched pattern, it applies one of three transformations:
- Case 1: Complete deletion of the suffix
- Case 2: Replacement with 'a' (using s_4) 
- Case 3: Replacement with 'e' (using s_5)

This specialized handling maintains Hungarian vowel harmony and morphological structure when removing sublative case markers.

## Parameters / Member Variables
- `*z`: Pointer to the SN_env structure containing the word being processed, cursor positions, and string boundaries
## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (searches for sublative patterns from array a_6 containing 'stul', 'astul', 'ástul', 'stül', 'estül', 'éstül')
  - [r_R1](r_R1.md) (checks if position is in R1 region)
  - [slice_del](../s/slice_del.md) (removes the matched suffix completely)
  - [slice_from_s](../s/slice_from_s.md) (replaces suffix with vowel 'a' or 'e')
- Called from (representative examples):
  - [hungarian_ISO_8859_2_stem](../h/hungarian_ISO_8859_2_stem.md)
  - [hungarian_UTF_8_stem](../h/hungarian_UTF_8_stem.md)

## Notes and Other Information
- The sublative case in Hungarian expresses "from/off of" relationships and can have complex morphophonological alternations
- The pre-check ensures minimum word length (4 characters) and 'l' ending to optimize pattern matching
- Different vowel harmony classes require different replacement vowels ('a' vs 'e')
- Returns 1 on successful processing, 0 if no pattern matches, and negative values on errors
- This function is part of the comprehensive Hungarian case system handling in PostgreSQL's full-text search
- The sublative case is less common than other cases but essential for complete morphological analysis