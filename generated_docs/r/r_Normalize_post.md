# r_Normalize_post

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:961-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L961-L1021)

## Overview
This function performs post-normalization processing for Arabic text in the PostgreSQL Snowball stemmer, applying final character standardizations after the main stemming operations.

## Definition
static int r_Normalize_post(struct SN_env * z)

## Detailed Description
The r_Normalize_post function is part of the Arabic UTF-8 stemming algorithm that handles post-processing normalization of Arabic text. It operates in two main phases:

1. **Backward Processing Phase**: Sets the cursor to the end of the string and searches backwards using lookup table a_1 (5 entries) to find patterns that need normalization. When found, it replaces them with a standardized 2-character sequence.

2. **Forward Processing Phase**: Iterates through the string from the current position, using lookup table a_2 (5 entries) to find patterns requiring normalization. It handles 3 different replacement cases, each replacing matched patterns with 2-character standardized sequences.

The function uses boundary markers (bra/ket) to define the text segments for replacement and employs UTF-8 aware character advancement to properly handle Arabic text encoding.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with the input string, cursor positions, and working buffers

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Searches backwards for patterns using lookup table a_1
  - [find_among](../f/find_among.md): Searches forward for patterns using lookup table a_2  
  - [slice_from_s](../s/slice_from_s.md): Replaces characters between bra and ket with specified string
  - [skip_utf8](../s/skip_utf8.md): Advances cursor by one UTF-8 character
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function at src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1655

## Notes and Other Information
This function is automatically generated code from Snowball stemming algorithms and complements r_Normalize_pre by handling normalization that must occur after stemming operations. It specifically targets Arabic character sequences that need standardization in the final output. The function processes UTF-8 encoded Arabic text and returns 1 on success or a negative value on error. The two-phase approach (backward then forward) ensures comprehensive normalization of the processed text.

## Simplified Source

```c
static int r_Normalize_post(struct SN_env * z) {
    // Phase 1: Backward processing from end of string
    int start_pos = z->c;
    z->lb = z->c;
    z->c = z->l; // Move to end

    // Look for patterns at end of string
    z->ket = z->c;
    if (z->c > z->lb + 1 &&
        z->p[z->c - 1] >> 5 == 5 &&
        ((124 >> (z->p[z->c - 1] & 0x1f)) & 1)) {

        if (find_among_b(z, a_1, 5)) {
            z->bra = z->c;
            slice_from_s(z, 2, normalized_string); // Replace with 2-char sequence
        }
    }
    z->c = start_pos; // Restore position

    // Phase 2: Forward processing through entire string
    int saved_pos = z->c;
    while (1) {
        int current_pos = z->c;
        int char_pos = z->c;

        // Check for normalization patterns
        z->bra = z->c;
        if (z->c + 1 < z->l &&
            z->p[z->c + 1] >> 5 == 5 &&
            ((124 >> (z->p[z->c + 1] & 0x1f)) & 1)) {

            int pattern = find_among(z, a_2, 5);
            if (pattern) {
                z->ket = z->c;
                switch (pattern) {
                    case 1:
                        slice_from_s(z, 2, replacement_1);
                        break;
                    case 2:
                        slice_from_s(z, 2, replacement_2);
                        break;
                    case 3:
                        slice_from_s(z, 2, replacement_3);
                        break;
                }
                continue;
            }
        }

        // No pattern found - advance one UTF-8 character
        z->c = char_pos;
        if (skip_utf8(z->p, z->c, z->l, 1) < 0) {
            break; // End of string
        }
        z->c = skip_utf8(z->p, z->c, z->l, 1);
    }

    z->c = saved_pos;
    return 1;
}
```