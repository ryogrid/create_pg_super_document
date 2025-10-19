# r_particle_etc

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_finnish.c:304-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_finnish.c#L304-L331)

## Overview
The r_particle_etc function removes Finnish particle suffixes and other small morphological elements from word endings during the stemming process.

## Definition
static int r_particle_etc(struct SN_env * z)

## Detailed Description
This function implements the Finnish particle removal step in the Snowball stemming algorithm. It identifies and removes small suffixes like particles, clitics, and other morphological markers that can be attached to Finnish words. The function operates within the R1 region boundary to ensure it only removes suffixes from appropriate positions.

The algorithm works by:
1. Setting up boundary limits using the R1 region (z->I[1])
2. Finding matching suffixes from a predefined list (a_0 with 10 entries)
3. Applying different rules based on the type of suffix found:
   - Case 1: Requires the suffix to be preceded by specific particle-ending characters
   - Case 2: Requires the suffix to be in the R2 region for removal
4. If conditions are met, the matched suffix is deleted

The function uses backward matching (find_among_b) to locate suffixes from the end of the word, which is typical for suffix-based morphological processing.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position
  - : R1 region boundary marker
  - : Left boundary for processing
  - : End position of matched substring
  - : Start position of matched substring
- : Local variable storing the type of suffix found (1 or 2)
- : Local variable storing the original left boundary

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function
  - [in_grouping_b](../i/in_grouping_b.md): Backward character group testing
  - [r_R2](r_R2.md): Region boundary test function
  - [slice_del](../s/slice_del.md): Function to delete the matched substring
  - g_particle_end: Character group defining valid particle endings
  - a_0: Array of particle suffix patterns (10 entries)
- Called from (representative examples):
  - [finnish_ISO_8859_1_stem](../f/finnish_ISO_8859_1_stem.md): Main Finnish stemming function
  - [finnish_UTF_8_stem](../f/finnish_UTF_8_stem.md): UTF-8 version of Finnish stemming

## Notes and Other Information
This function is specific to Finnish morphology and handles the complex particle system in Finnish language. Finnish has many clitics and particles that can be attached to words, and this function ensures they are properly identified and removed during text normalization. The function returns 1 on successful removal, 0 if no applicable suffix is found, and negative values for errors. The two-case structure reflects different classes of particles with different removal conditions - some require specific preceding characters while others require positioning in deeper morphological regions.

## Simplified Source

```c
static int r_particle_etc(struct SN_env * z) {
    int suffix_type;

    // Set boundaries to R1 region for particle processing
    if (z->c < z->I[1]) return 0;
    int saved_boundary = z->lb;
    z->lb = z->I[1];

    // Find particle suffix from predefined list
    z->ket = z->c;
    suffix_type = find_among_b(z, a_0, 10);
    if (!suffix_type) {
        z->lb = saved_boundary;
        return 0;
    }
    z->bra = z->c;
    z->lb = saved_boundary;

    // Apply removal rules based on suffix type
    switch (suffix_type) {
        case 1:
            // Type 1: Check if preceded by valid particle ending characters
            if (in_grouping_b(z, g_particle_end, 97, 246, 0)) return 0;
            break;
        case 2:
            // Type 2: Must be in R2 region
            if (r_R2(z) <= 0) return 0;
            break;
    }

    // Delete the matched particle suffix
    slice_del(z);
    return 1;
}
```