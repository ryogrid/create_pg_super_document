# regex_selectivity_sub

## Location
[src/backend/utils/adt/like_support.c:1360-1454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1360-L1454)

## Overview
A recursive function that estimates the selectivity of regular expression patterns by parsing and analyzing regex metacharacters and their associated selectivity factors.

## Definition
```c
static Selectivity regex_selectivity_sub(const char *patt, int pattlen, bool case_insensitive)
```

## Detailed Description
This function implements a recursive regular expression parser that calculates selectivity estimates by analyzing regex metacharacters and applying appropriate selectivity factors. The function handles the complex syntax of regular expressions including:

**Core Regex Elements:**
- **Parentheses**: Recursively processes grouped subexpressions
- **Alternation (`|`)**: Sums probabilities of alternative patterns at the same nesting level
- **Character classes (`[...]`)**: Handles both positive and negative character classes
- **Dot (`.`)**: Matches any single character
- **Quantifiers**: `*`, `+`, `?` and `{n,m}` patterns
- **Escaped characters**: Backslash-quoted literals

**Algorithm Logic:**
- Maintains parenthesis depth tracking to properly handle nested expressions
- For alternation (`|`), splits the pattern and recursively sums selectivities 
- For character classes, applies CHAR_RANGE_SEL or its complement for negated classes
- Quantifiers are treated with PARTIAL_WILDCARD_SEL (simplified approach)
- Only applies selectivity factors when at parenthesis depth 0 (top level)

The function includes stack depth checking to prevent stack overflow from deeply nested recursive calls.

## Parameters / Member Variables
- `patt`: Pointer to the regular expression pattern string to analyze
- `pattlen`: Length of the pattern string in characters
- `case_insensitive`: Boolean flag for case-insensitive matching (affects selectivity factors)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (prevents stack overflow in deep recursion)
  - [regex_selectivity_sub](regex_selectivity_sub.md) (recursive self-calls for subpatterns and alternatives)
  - CHAR_RANGE_SEL (selectivity constant for character classes)
  - ANY_CHAR_SEL (selectivity constant for dot metacharacter)
  - PARTIAL_WILDCARD_SEL (selectivity constant for quantifiers)
  - FIXED_CHAR_SEL (selectivity constant for literal characters)
- Called from:
  - [regex_selectivity](regex_selectivity.md) (main entry point for regex selectivity estimation)
  - [regex_selectivity_sub](regex_selectivity_sub.md) (recursive calls for subpatterns)

## Notes and Other Information
- This is a static function within like_support.c, not exposed in the public API
- The function uses a simplified approach for quantifiers, treating `*`, `+`, `?`, and `{n,m}` all with the same PARTIAL_WILDCARD_SEL factor
- Character class parsing handles edge cases like `]` as the first character after `[` or `[^`
- Parenthesis depth tracking ensures that selectivity factors are only applied at the top level, avoiding double-counting for nested expressions
- The alternation handling uses addition rather than multiplication, reflecting the OR semantics
- Final selectivity is clamped to 1.0 to handle mathematical overflow from multiple wildcards
- The recursive design allows handling arbitrarily complex nested regex patterns
- Stack depth checking prevents crashes on pathological regex patterns with deep nesting

## Simplified Source
```c
static Selectivity regex_selectivity_sub(const char *pattern, int pattern_length, bool case_insensitive) {
    Selectivity selectivity = 1.0;
    int paren_depth = 0;
    int paren_start_pos = 0;
    int pos;

    // Prevent stack overflow from deep recursion
    check_stack_depth();

    for (pos = 0; pos < pattern_length; pos++) {
        if (pattern[pos] == '(') {
            // Track parenthesis nesting
            if (paren_depth == 0)
                paren_start_pos = pos;
            paren_depth++;
        }
        else if (pattern[pos] == ')' && paren_depth > 0) {
            // Process grouped subexpression recursively
            paren_depth--;
            if (paren_depth == 0) {
                selectivity *= regex_selectivity_sub(pattern + (paren_start_pos + 1),
                                                   pos - (paren_start_pos + 1),
                                                   case_insensitive);
            }
        }
        else if (pattern[pos] == '|' && paren_depth == 0) {
            // Alternation: sum probabilities of alternatives
            selectivity += regex_selectivity_sub(pattern + (pos + 1),
                                               pattern_length - (pos + 1),
                                               case_insensitive);
            break; // Rest of pattern handled recursively
        }
        else if (pattern[pos] == '[') {
            // Character class: [abc] or [^abc]
            bool negated = false;

            if (pattern[++pos] == '^') {
                negated = true;
                pos++;
            }
            if (pattern[pos] == ']') // ']' at start is literal
                pos++;

            // Skip to end of character class
            while (pos < pattern_length && pattern[pos] != ']')
                pos++;

            if (paren_depth == 0) {
                selectivity *= negated ? (1.0 - CHAR_RANGE_SEL) : CHAR_RANGE_SEL;
            }
        }
        else if (pattern[pos] == '.') {
            // Dot: matches any character
            if (paren_depth == 0)
                selectivity *= ANY_CHAR_SEL;
        }
        else if (pattern[pos] == '*' || pattern[pos] == '?' || pattern[pos] == '+') {
            // Quantifiers: simplified treatment
            if (paren_depth == 0)
                selectivity *= PARTIAL_WILDCARD_SEL;
        }
        else if (pattern[pos] == '{') {
            // Quantifier range: {n,m}
            while (pos < pattern_length && pattern[pos] != '}')
                pos++;
            if (paren_depth == 0)
                selectivity *= PARTIAL_WILDCARD_SEL;
        }
        else if (pattern[pos] == '\\') {
            // Escaped character: treat as literal
            pos++;
            if (pos < pattern_length && paren_depth == 0)
                selectivity *= FIXED_CHAR_SEL;
        }
        else {
            // Regular character: literal match
            if (paren_depth == 0)
                selectivity *= FIXED_CHAR_SEL;
        }
    }

    // Clamp selectivity to valid range
    if (selectivity > 1.0)
        selectivity = 1.0;

    return selectivity;
}
```