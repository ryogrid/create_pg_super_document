# regexeqsel

## Location
[src/backend/utils/adt/like_support.c:793-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L793-L801)

## Overview
A PostgreSQL SQL-callable selectivity estimation function specifically for regular expression pattern matching operations using the ~ (regex match) operator.

## Definition
```c
Datum regexeqsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regexeqsel` function is a PostgreSQL system function that provides selectivity estimation for regular expression pattern matching queries. It serves as the entry point for cost-based optimization when the query planner encounters the ~ (regex match) operator in WHERE clauses and other filtering contexts.

When PostgreSQL's query planner evaluates a query like `SELECT * FROM table WHERE column ~ 'pattern'`, it calls `regexeqsel` to estimate what fraction of rows will match the regular expression. This estimate is crucial for:
- Choosing between different query execution plans
- Deciding whether to use indexes or sequential scans
- Ordering joins in multi-table queries
- Estimating overall query cost and execution time

The function delegates the actual selectivity calculation to the `patternsel` adapter function, which in turn uses the sophisticated pattern analysis logic in `patternsel_common`. This layered design allows the same core estimation algorithms to be shared across different pattern matching operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call interface containing:
  - `PlannerInfo*`: Query planner context and table statistics
  - `Oid`: Operator OID for the ~ (regex match) operator
  - `List*`: Arguments to the regex operation (typically column and pattern)
  - `int32`: Variable relation ID for statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - [patternsel](../p/patternsel.md): Pattern selectivity adapter function
  - `Pattern_Type_Regex`: Enum constant specifying regex pattern type
- Called from (representative examples):
  - PostgreSQL query planner when encountering ~ operators
  - Cost estimation routines during query optimization
  - Selectivity calculation in complex query plans

## Notes and Other Information
- Returns a `Datum` containing a float8 selectivity value between 0.0 and 1.0
- Specifically handles positive regex matching (not negated patterns like !~)
- Part of PostgreSQL's comprehensive cost-based optimization system
- Works in conjunction with index support functions to enable efficient regex queries
- The selectivity estimate directly influences whether PostgreSQL will use indexes, perform sequential scans, or choose other execution strategies
- Registered in PostgreSQL's system catalogs as the selectivity function for the ~ operator
- Uses the same underlying pattern analysis as LIKE operations but handles full regular expression syntax