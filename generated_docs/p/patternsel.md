# patternsel

## Location
[src/backend/utils/adt/like_support.c:760-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L760-L792)

## Overview
A SQL-callable adapter function that bridges the PostgreSQL function call interface with the internal `patternsel_common` selectivity estimation engine for pattern matching operations.

## Definition
```c
static double patternsel(PG_FUNCTION_ARGS, Pattern_Type ptype, bool negate)
```

## Detailed Description
The `patternsel` function serves as an impedance-matching layer between PostgreSQL's SQL function calling convention and the internal `patternsel_common` function. It extracts the standard selectivity function arguments from the PostgreSQL function call interface and forwards them to the core pattern selectivity estimation logic.

Key responsibilities include:
1. **Argument Extraction**: Unpacks the standard selectivity function arguments (planner info, operator OID, arguments list, relation ID) from the PostgreSQL function call format
2. **Collation Handling**: Automatically retrieves the appropriate collation context using `PG_GET_COLLATION()`
3. **Negation Processing**: For NOT LIKE and similar negated operators, looks up the corresponding positive operator using `get_negator()` before passing to the core logic
4. **Interface Translation**: Converts the function call interface to the direct function call expected by `patternsel_common`

This design allows the same core selectivity logic to be used by various pattern matching operators (LIKE, ILIKE, regex, etc.) while maintaining the standard PostgreSQL selectivity function interface.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call arguments containing:
  - `arg0`: `PlannerInfo*` - Planner context and statistics
  - `arg1`: `Oid` - Operator OID for the pattern operation
  - `arg2`: `List*` - Arguments to the pattern operation
  - `arg3`: `int32` - Variable relation ID for statistics lookup
- `ptype`: `Pattern_Type` enum specifying the type of pattern matching (LIKE, regex, etc.)
- `negate`: Boolean indicating whether this is a negated operation (NOT LIKE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GET_COLLATION`: Retrieves the current collation context
  - [get_negator](../g/get_negator.md): Finds the positive operator corresponding to a negated operator
  - [patternsel_common](patternsel_common.md): Core pattern selectivity estimation function
- Called from (representative examples):
  - [regexeqsel](../r/regexeqsel.md): Selectivity function for regex match operator
  - [icregexeqsel](../i/icregexeqsel.md): Selectivity for case-insensitive regex
  - [likesel](../l/likesel.md): Selectivity function for LIKE operator
  - [prefixsel](prefixsel.md): Selectivity for prefix matching
  - [iclikesel](../i/iclikesel.md): Selectivity for ILIKE operator
  - [regexnesel](../r/regexnesel.md): Selectivity for negated regex
  - [nlikesel](../n/nlikesel.md): Selectivity for NOT LIKE operator

## Notes and Other Information
- This is a static utility function used only within the like_support.c module
- Provides a standardized interface for all pattern-based selectivity functions
- Handles the complexity of negated operators by automatically looking up the positive counterpart
- Throws an error if a negated operator is used without a corresponding positive operator
- Passes `InvalidOid` for the function OID parameter, letting `patternsel_common` derive it from the operator OID when needed
- Forms part of PostgreSQL's cost-based optimization system by providing accurate selectivity estimates for pattern matching operations

## Simplified Source

```c
static double patternsel(PG_FUNCTION_ARGS, Pattern_Type ptype, bool negate) {
    // Extract standard selectivity function arguments
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    int varRelid = PG_GETARG_INT32(3);
    Oid collation = PG_GET_COLLATION();

    // Handle negated operators by finding the positive counterpart
    if (negate) {
        operator = get_negator(operator);
        if (!OidIsValid(operator))
            elog(ERROR, "patternsel called for operator without a negator");
    }

    // Forward to core selectivity estimation logic
    return patternsel_common(root, operator, InvalidOid, args,
                           varRelid, collation, ptype, negate);
}
```