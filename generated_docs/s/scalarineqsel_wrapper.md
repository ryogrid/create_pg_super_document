# scalarineqsel_wrapper

## Location
[src/backend/utils/adt/selfuncs.c:1401-1471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1401-L1471)

## Overview
A common wrapper function for selectivity estimators that handles the preprocessing and validation before invoking the core  function for inequality selectivity estimation.

## Definition

```c
static Datum
scalarineqsel_wrapper(PG_FUNCTION_ARGS, bool isgt, bool iseq)
```
## Detailed Description
The  function serves as a unified preprocessing layer for PostgreSQL's inequality selectivity estimation functions (, , , ). It handles the common validation, argument processing, and setup logic that all inequality estimators need before delegating the actual selectivity calculation to . The function extracts and validates the restriction clause components, ensures the variable is positioned on the left side of the comparison, and handles edge cases like NULL constants and missing commutators.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing planner info, operator OID, argument list, and variable relation ID
- : Boolean indicating if this is a greater-than type operation ( or )
- : Boolean indicating if this is an equality-inclusive operation ( or )

The function extracts these from PG_FUNCTION_ARGS:
- : PlannerInfo pointer containing planner context
- : OID of the comparison operator
- : List of arguments to the operator
- : Relation ID of the variable being compared
- : Collation to use for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts variable and constant from restriction clause
  - : Gets the commutator operator when variable needs to be on left
  - : Core function that performs the actual selectivity estimation
  - : Cleans up variable statistics data
  - : Macro to extract collation from function call
  - : Default selectivity constant for inequality operations
- Called from:
  - : Less-than selectivity estimator (src/backend/utils/adt/selfuncs.c:1474)
  - : Less-than-or-equal selectivity estimator (src/backend/utils/adt/selfuncs.c:1483)  
  - : Greater-than selectivity estimator (src/backend/utils/adt/selfuncs.c:1492)
  - : Greater-than-or-equal selectivity estimator (src/backend/utils/adt/selfuncs.c:1501)

## Notes and Other Information
- This is a static helper function that eliminates code duplication among the four inequality selectivity estimators
- Returns  (default inequality selectivity) when the restriction cannot be processed
- Returns 0.0 selectivity when comparing against NULL constants (assuming strict operators)
- Automatically handles operator commutation to ensure the variable appears on the left side of comparisons
- The function performs several validation checks before delegating to  for the actual statistical analysis
- Part of PostgreSQL's cost-based query optimizer infrastructure for estimating predicate selectivity