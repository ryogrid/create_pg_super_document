# pattern_fixed_prefix

## Location
[src/backend/utils/adt/like_support.c:1167-1231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1167-L1231)

## Overview
Extracts the fixed prefix portion from different types of pattern expressions (LIKE, regex, prefix) to optimize string matching operations by enabling index usage and calculating selectivity estimates.

## Definition
```c
static Pattern_Prefix_Status pattern_fixed_prefix(Const *patt, Pattern_Type ptype, Oid collation,
                                                  Const **prefix, Selectivity *rest_selec)
```

## Detailed Description
This function serves as a central dispatcher for extracting fixed prefixes from various pattern types used in PostgreSQL string matching operations. It analyzes the input pattern and delegates to appropriate specialized functions based on the pattern type. The extracted prefix can be used by the query planner to enable index scans and calculate more accurate selectivity estimates for WHERE clauses involving pattern matching.

The function handles five different pattern types:
- **Pattern_Type_Like**: Standard LIKE patterns with % and _ wildcards
- **Pattern_Type_Like_IC**: Case-insensitive LIKE patterns (ILIKE)
- **Pattern_Type_Regex**: Regular expression patterns
- **Pattern_Type_Regex_IC**: Case-insensitive regular expression patterns
- **Pattern_Type_Prefix**: Simple prefix patterns (optimization case)

For prefix patterns, the function handles the trivial case directly by copying the entire pattern as the prefix, since prefix patterns by definition have no variable portions.

## Parameters / Member Variables
- `patt`: Const pointer to the pattern constant expression to analyze
- `ptype`: Enumeration specifying the type of pattern (LIKE, regex, prefix, etc.)
- `collation`: OID of the collation to use for pattern matching operations
- `prefix`: Output parameter - pointer to receive the extracted fixed prefix constant
- `rest_selec`: Output parameter - pointer to receive selectivity estimate for the remaining pattern portion after the prefix

## Dependencies
- Functions called/Symbols referenced:
  - [like_fixed_prefix](../l/like_fixed_prefix.md) (for LIKE and ILIKE patterns)
  - [regex_fixed_prefix](../r/regex_fixed_prefix.md) (for regex patterns)
  - [makeConst](../m/makeConst.md) (to create the prefix constant for Pattern_Type_Prefix)
  - [datumCopy](../d/datumCopy.md) (to copy pattern data for prefix patterns)
- Called from:
  - [match_pattern_prefix](../m/match_pattern_prefix.md)
  - [patternsel_common](patternsel_common.md)

## Notes and Other Information
- This is a static function within like_support.c, not exposed in the public API
- The function returns Pattern_Prefix_Status indicating whether a complete prefix was found, partial prefix, or no prefix
- For Pattern_Type_Prefix cases, rest_selec is set to 1.0 since the entire pattern is the prefix
- The function uses a switch statement for efficient pattern type dispatch
- Error handling includes an elog(ERROR) for unrecognized pattern types

## Simplified Source

```c
static Pattern_Prefix_Status
pattern_fixed_prefix(Const *patt, Pattern_Type ptype, Oid collation,
                     Const **prefix, Selectivity *rest_selec)
{
    Pattern_Prefix_Status result;

    switch (ptype) {
        case Pattern_Type_Like:
            // Standard LIKE pattern analysis
            result = like_fixed_prefix(patt, false, collation, prefix, rest_selec);
            break;

        case Pattern_Type_Like_IC:
            // Case-insensitive LIKE pattern analysis
            result = like_fixed_prefix(patt, true, collation, prefix, rest_selec);
            break;

        case Pattern_Type_Regex:
            // Regular expression pattern analysis
            result = regex_fixed_prefix(patt, false, collation, prefix, rest_selec);
            break;

        case Pattern_Type_Regex_IC:
            // Case-insensitive regex pattern analysis
            result = regex_fixed_prefix(patt, true, collation, prefix, rest_selec);
            break;

        case Pattern_Type_Prefix:
            // Trivial case: entire pattern is the prefix
            result = Pattern_Prefix_Partial;
            *prefix = makeConst(patt->consttype, patt->consttypmod, patt->constcollid,
                               patt->constlen, datumCopy(patt->constvalue,
                               patt->constbyval, patt->constlen),
                               patt->constisnull, patt->constbyval);
            if (rest_selec != NULL)
                *rest_selec = 1.0;  // Entire pattern matches
            break;

        default:
            elog(ERROR, "unrecognized ptype: %d", (int) ptype);
            result = Pattern_Prefix_None;  // Keep compiler quiet
            break;
    }

    return result;
}
```