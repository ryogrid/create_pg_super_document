# regex_selectivity_sub

## Location
src/backend/utils/adt/like_support.c: 1360 - 1454

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
  - check_stack_depth (prevents stack overflow in deep recursion)
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