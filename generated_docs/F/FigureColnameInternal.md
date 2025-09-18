# FigureColnameInternal

## Location
[src/backend/parser/parse_target.c:1743-2033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1743-L2033)

## Overview
FigureColnameInternal is the internal workhorse function for FigureColname that determines appropriate column names for SQL expressions by analyzing parse tree nodes and returning a confidence level for the chosen name.

## Definition


## Detailed Description
This function recursively traverses PostgreSQL parse tree nodes to extract meaningful column names from various SQL expression types. It implements a confidence-based naming system where different node types and naming contexts yield different confidence levels:

- **0**: No information available
- **1**: Second-best name choice (fallback options)  
- **2**: Good name choice (preferred options)

The function handles a comprehensive set of SQL expression types including column references, function calls, type casts, subqueries, case expressions, XML functions, JSON functions, and SQL value functions. For complex expressions, it attempts to find the most meaningful identifier, often recursing into sub-expressions when direct naming isn't available.

## Parameters / Member Variables
- : The parse tree node to analyze for column name extraction
- : Output parameter - pointer to char pointer that will be set to the chosen column name if a suitable name is found (only modified when return value > 0)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (for node type identification)
  - strVal (for extracting string values)
  - llast (for getting last list element)
  - linitial (for getting first list element)
  - elog (for error reporting)
  - IsA (for type checking)
  - lfirst (for list iteration)
  - Various node type constants (T_ColumnRef, T_FuncCall, etc.)
  - Various enum constants (AEXPR_NULLIF, SVFOP_*, IS_*, JSON_*_OP)

- Called from (representative examples):
  - [FigureColname](FigureColname.md) (main public interface)
  - [FigureIndexColname](FigureIndexColname.md) (for index column naming)
  - [FigureColnameInternal](FigureColnameInternal.md) (recursive self-calls for complex expressions)

## Notes and Other Information
- This is a static function, only accessible within src/backend/parser/parse_target.c
- The function uses a large switch statement to handle different node types, with each case implementing specific logic for extracting meaningful names
- For function calls, it uses the function name as the column name
- For column references and indirections, it extracts the rightmost field name
- Special handling exists for SQL standard functions (NULLIF, GROUPING, MERGE_ACTION) and built-in value functions (CURRENT_DATE, etc.)
- JSON and XML functions are given descriptive names based on their operation type
- The confidence scoring system allows callers to prefer higher-confidence naming choices
- Recursive calls are used for wrapped expressions like TypeCast and CollateClause to unwrap and find the underlying meaningful name