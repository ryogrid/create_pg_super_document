# greek_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3461-3669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3461-L3669)

## Overview
The main entry point function for the Snowball Greek language stemming algorithm that processes UTF-8 encoded Greek text to reduce words to their morphological roots through a systematic series of transformation steps.

## Definition
```c
extern int greek_UTF_8_stem(struct SN_env * z)
```

## Detailed Description
This is the primary stemming function for Greek language text processing in PostgreSQL's full-text search functionality. It implements the complete Snowball Greek stemming algorithm through a carefully orchestrated sequence of morphological transformation steps:

**Initialization Phase:**
1. Sets up cursor positions (lb=c, c=l for backward processing)
2. Converts text to lowercase using r_tolower
3. Validates minimum word length with r_has_min_length
4. Sets step counter I[0] = 1 to enable subsequent conditional steps

**Multi-Phase Stemming Process:**
The function executes 33 distinct stemming steps in a specific order, each using cursor position backtracking (m1-m33 variables) to ensure non-destructive attempts:

- **Steps 1-10**: Basic morphological transformations (r_step1, r_steps1-10)
- **Steps 2a-2d**: Specialized secondary transformations  
- **Steps 3-4**: Intermediate morphological processing
- **Steps 5a-5m**: Extensive suffix and ending transformations (including r_step5l, r_step5m)
- **Step 6**: Advanced conditional transformations based on step counter state
- **Step 7**: Final comparative/superlative form handling

Each step uses backtracking to attempt transformations without commitment - if a step fails, the cursor position is restored and processing continues.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing:
  - `lb`: Left boundary (start position)
  - `c`: Current cursor position  
  - `l`: String length (end position)
  - `I[0]`: Step counter/flag for conditional operations

## Dependencies
- Functions called/Symbols referenced:
  - [r_tolower](../r/r_tolower.md) (case normalization)
  - [r_has_min_length](../r/r_has_min_length.md) (length validation)
  - [r_step1](../r/r_step1.md), r_steps1-r_steps10 (basic morphological steps)
  - [r_step2a](../r/r_step2a.md), r_step2b, r_step2c, r_step2d (secondary steps)
  - [r_step3](../r/r_step3.md), r_step4 (intermediate steps)  
  - [r_step5a](../r/r_step5a.md) through r_step5m (extensive suffix processing)
  - [r_step6](../r/r_step6.md) (conditional transformations)
  - [r_step7](../r/r_step7.md) (comparative/superlative handling)
- Called from:
  - External interface (no internal references found)

## Notes and Other Information
- Returns 1 on successful stemming completion (always succeeds if input is valid)
- Returns negative values on internal errors from sub-functions
- Implements the complete Greek Snowball stemming algorithm specification
- Handles complex Greek morphology including verb conjugations, noun declensions, and adjective forms
- Uses UTF-8 encoding throughout for proper Greek character handling
- The backtracking mechanism (m1-m33) ensures that failed transformations don't affect the text
- [Step](../S/Step.md) counter (I[0]) creates dependencies between steps for proper morphological analysis
- This is the main public interface for Greek stemming in PostgreSQL's text search system
- Part of the libstemmer library integrated into PostgreSQL for multilingual full-text search support

## Simplified Source

```c
extern int greek_UTF_8_stem(struct SN_env * z) {
    // Phase 1: Initialize for backward processing and normalize
    z->lb = z->c;
    z->c = z->l;  // Start from end for suffix processing

    // Convert to lowercase and validate minimum length
    r_tolower(z);
    if (r_has_min_length(z) <= 0) return 0;

    // Enable conditional steps
    z->I[0] = 1;

    // Phase 2: Execute systematic stemming steps with backtracking
    // Each step saves position and restores if needed

    // Basic morphological transformations (Steps 1-10)
    TRY_STEP(r_step1);
    TRY_STEP(r_steps1);  TRY_STEP(r_steps2);  TRY_STEP(r_steps3);
    TRY_STEP(r_steps4);  TRY_STEP(r_steps5);  TRY_STEP(r_steps6);
    TRY_STEP(r_steps7);  TRY_STEP(r_steps8);  TRY_STEP(r_steps9);
    TRY_STEP(r_steps10);

    // Secondary transformations (Steps 2a-2d)
    TRY_STEP(r_step2a);  TRY_STEP(r_step2b);
    TRY_STEP(r_step2c);  TRY_STEP(r_step2d);

    // Intermediate processing (Steps 3-4)
    TRY_STEP(r_step3);   TRY_STEP(r_step4);

    // Extensive suffix processing (Steps 5a-5m)
    TRY_STEP(r_step5a);  TRY_STEP(r_step5b);  TRY_STEP(r_step5c);
    TRY_STEP(r_step5d);  TRY_STEP(r_step5e);  TRY_STEP(r_step5f);
    TRY_STEP(r_step5g);  TRY_STEP(r_step5h);  TRY_STEP(r_step5j);
    TRY_STEP(r_step5i);  TRY_STEP(r_step5k);  TRY_STEP(r_step5l);
    TRY_STEP(r_step5m);

    // Final transformations (Steps 6-7)
    TRY_STEP(r_step6);   // Conditional cleanup
    TRY_STEP(r_step7);   // Comparative/superlative forms

    // Phase 3: Finalize - reset cursor to start
    z->c = z->lb;
    return 1;
}

// Macro definition for the backtracking pattern used in each step:
// #define TRY_STEP(func) { int saved = z->l - z->c; func(z); z->c = z->l - saved; }
```