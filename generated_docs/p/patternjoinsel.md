# patternjoinsel

## Location
src/backend/utils/adt/like_support.c: 875 - 884

## Overview
A generic function for estimating selectivity of pattern-match join operations in PostgreSQL's query planner.

## Definition
```c
static double patternjoinsel(PG_FUNCTION_ARGS, Pattern_Type ptype, bool negate)
```

## Detailed Description
The `patternjoinsel` function provides a basic selectivity estimate for pattern-matching operations in join contexts. Currently, it uses a simple heuristic approach, returning a default selectivity value without analyzing the actual pattern. The function serves as a fallback for join selectivity estimation when more sophisticated analysis is not implemented. For negated patterns, it returns the complement of the default selectivity.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (currently unused in implementation)
- `ptype`: Pattern_Type enum value indicating the type of pattern (LIKE, ILIKE, regex, etc.)
- `negate`: Boolean flag indicating whether this is for a negated operation

## Dependencies
- Functions called/Symbols referenced:
  - `DEFAULT_MATCH_SEL` - Default selectivity constant (0.005)
  - `Pattern_Type` - Enum type for pattern matching types
- Called from (representative examples):
  - `regexeqjoinsel` - Regular expression join selectivity
  - `icregexeqjoinsel` - Case-insensitive regex join selectivity
  - `likejoinsel` - LIKE pattern join selectivity
  - `prefixjoinsel` - Prefix pattern join selectivity
  - `iclikejoinsel` - Case-insensitive LIKE join selectivity
  - `regexnejoinsel` - Negated regex join selectivity
  - `icregexnejoinsel` - Negated case-insensitive regex join selectivity
  - `nlikejoinsel` - Negated LIKE join selectivity
  - `icnlikejoinsel` - Negated case-insensitive LIKE join selectivity

## Notes and Other Information
- Returns 0.005 (DEFAULT_MATCH_SEL) for normal patterns or 0.995 (1.0 - DEFAULT_MATCH_SEL) for negated patterns
- Currently uses a simple `punt` approach with constant values rather than analyzing actual patterns
- This is a static function, only callable within the same source file
- Part of PostgreSQL's join selectivity estimation framework
- Located in `src/backend/utils/adt/like_support.c:875-884`
- The comment `For the moment we just punt` indicates this is a placeholder implementation