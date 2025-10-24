# string_matches_pattern

## Location
[src/test/regress/pg_regress.c:541-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L541-L614)

## Overview
Performs pattern matching for strings using a simplified regex implementation that supports basic wildcards, originally replacing shell script expr(1) functionality in pg_regress.

## Definition

```c
static bool
string_matches_pattern(const char *str, const char *pattern)
```
## Detailed Description
This function implements a simplified pattern matching algorithm that mimics the behavior of the original shell script's expr(1) usage. It supports two metacharacters: "." (single character wildcard) and ".*" (zero or more characters wildcard). The pattern matching assumes an implicit "^" at the start of the pattern (must match from beginning of string) but no implicit "$" at the end (pattern can match partial string). The implementation uses recursion for handling ".*" patterns with optimization to prevent unnecessary recursive calls. It's specifically designed for use with result mapping in regression tests.

## Parameters / Member Variables
- : The input string to match against the pattern
- : The pattern string containing literal characters and wildcards ("." and ".*")
- Returns:  - true if the string matches the pattern, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [string_matches_pattern](string_matches_pattern.md) (recursive self-call)
- Called from (representative examples):
  - [load_resultmap](../l/load_resultmap.md)
  - [string_matches_pattern](string_matches_pattern.md) (recursive)

## Notes and Other Information
- Function is marked static (internal to pg_regress.c)
- Implements only basic regex functionality: "." and ".*" metacharacters
- Designed to replace expr(1) usage from original shell script implementation
- Includes optimization to reduce recursion by checking first character match before recursing
- Handles edge cases like trailing ".*" patterns and end-of-string conditions
- Used primarily for resultmap pattern matching in regression testing
- Pattern matching starts at string beginning (implicit "^" anchor)
- No implicit end-of-string anchor (no "$" at pattern end)
- Could be extended to support more regex features if needed
- Located in src/test/regress/pg_regress.c:541-614

## Simplified Source

```c
static bool string_matches_pattern(const char *str, const char *pattern) {
    while (*str && *pattern) {
        // Handle ".*" pattern (zero or more characters)
        if (*pattern == '.' && pattern[1] == '*') {
            pattern += 2;

            // Trailing ".*" matches everything
            if (*pattern == '\0')
                return true;

            // Find position where rest of pattern can match
            while (*str) {
                // Optimization: only recurse if first chars might match
                if (*str == *pattern || *pattern == '.') {
                    if (string_matches_pattern(str, pattern))
                        return true;
                }
                str++;
            }
            return false;  // No match found
        }
        // Handle single char wildcard "." or literal match
        else if (*pattern != '.' && *str != *pattern) {
            return false;  // No match
        }

        str++;
        pattern++;
    }

    // Check if we've consumed all pattern
    if (*pattern == '\0')
        return true;

    // Handle remaining ".*" patterns at end
    while (*pattern == '.' && pattern[1] == '*')
        pattern += 2;

    return (*pattern == '\0');
}
```