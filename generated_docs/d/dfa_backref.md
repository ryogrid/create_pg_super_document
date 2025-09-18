# dfa_backref

## Location
[src/backend/regex/rege_dfa.c:506-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L506-L584)

## Overview
Finds the best match length for a known backreference string without running the full NFA, providing optimized backref matching.

## Definition
```c
static chr *
dfa_backref(struct vars *v,
            struct dfa *d,
            chr *start,        /* where the match should start */
            chr *min,          /* match must end at or after here */
            chr *max,          /* match must end at or before here */
            bool shortest)
```

## Detailed Description
The `dfa_backref` function provides an optimized path for matching backreferences when the referenced string is already known. Instead of executing the backref nodes full NFA, it directly compares the target string against repeated instances of the backreferenced substring. This approach is significantly more efficient than general NFA execution. The function handles repetition bounds (min/max), supports both shortest and longest matching preferences, and includes special handling for zero-length backreferences.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and match results
- `d`: Pointer to DFA structure containing backref metadata (backno, backmin, backmax)
- `start`: Starting character position where backreference matching should begin
- `min`: Minimum ending position - match must end at or after this point
- `max`: Maximum ending position - match must end at or before this point
- `shortest`: Boolean flag indicating whether to prefer shortest or longest match

## Dependencies
- Functions called/Symbols referenced:
  - [compare](../c/compare.md) (string comparison function from regex globals)
  - [shortest](../s/shortest.md) (called recursively for some backref scenarios)
  - DUPINF (infinity constant for unlimited repetitions)
- Called from (representative examples):
  - [longest](../l/longest.md) (longest-match engine for backref optimization)
  - [shortest](../s/shortest.md) (shortest-match engine for backref optimization)
  - LOFF (regex execution offset function)

## Notes and Other Information
- Synchronized with cbrdissect() which performs similar validation for predetermined string sections
- Handles zero-length backreferences as special case (matches empty string with any repetition count)
- Calculates valid repetition ranges based on string length and position constraints
- Performs actual string content comparison only after validating repetition bounds
- Essential optimization for backref performance in PostgreSQL regex engine
- Returns match endpoint for valid matches, NULL for invalid or impossible matches