# r_remove_command_suffixes

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_tamil.c:1105-1123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_tamil.c#L1105-L1123)

## Overview
Removes Tamil imperative/command suffixes from words after validating minimum length and specific character patterns.

## Definition

```c
}

static int r_remove_command_suffixes(struct SN_env * z)
```
## Detailed Description
This function handles the removal of Tamil imperative or command suffixes from verbs. The process involves:

1. Validating minimum word length through r_has_min_length to prevent over-stemming
2. Setting up backward processing from the word end
3. Performing a specific character check (looking for character value 191 at position c-1)
4. Using pattern matching to identify command suffix patterns from array a_15 (containing 2 patterns)
5. Completely removing the matched suffix using slice_del (no replacement, unlike other suffix functions)
6. Setting the success flag and returning to the start position

This function is more restrictive than other suffix removal functions, requiring both length and specific character validation.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : State flag set to 1 when a command suffix is successfully removed
  - //: Cursor positions for boundary/current/limit tracking
  - /: Bracket positions marking the suffix for deletion
  - : Pointer to character array (used for character value validation)

## Dependencies
- Functions called/Symbols referenced:
  - [r_has_min_length](r_has_min_length.md) (ensures minimum word length before processing)
  - [find_among_b](../f/find_among_b.md) (backward pattern matching using array a_15 with 2 command patterns)
  - [slice_del](../s/slice_del.md) (completely removes the matched suffix)
- Called from (representative examples):
  - [tamil_UTF_8_stem](../t/tamil_UTF_8_stem.md) (main Tamil stemming function)

## Notes and Other Information
- Specifically targets Tamil imperative/command verb forms
- More restrictive than other suffix functions due to additional character validation (checks for character 191)
- Uses complete suffix removal rather than replacement, indicating command suffixes don't need morphological transformation
- The a_15 array contains only 2 command suffix patterns, suggesting these are highly specific morphological markers
- Part of Tamil verb stemming pipeline that handles different verb aspects and moods
- Character validation (191) likely corresponds to specific Tamil Unicode characters used in command forms

## Simplified Source

```c
static int r_remove_command_suffixes(struct SN_env * z) {
    // Check minimum word length before processing
    int ret = r_has_min_length(z);
    if (ret <= 0) return ret;

    // Initialize state flag
    z->I[1] = 0;

    // Set up backward processing boundaries
    z->lb = z->c;
    z->c = z->l;

    // Validate specific character pattern for command suffixes
    z->ket = z->c;
    if (z->c - 5 <= z->lb || z->p[z->c - 1] != 191) {
        return 0; // Required character pattern not found
    }

    // Look for command suffix patterns (only 2 patterns in a_15)
    if (!find_among_b(z, a_15, 2)) {
        return 0; // No command suffix patterns matched
    }

    // Command suffix found - remove it completely
    z->bra = z->c;
    slice_del(z);

    // Mark successful processing and reset position
    z->I[1] = 1;
    z->c = z->lb;

    return 1; // Success
}
```