# ParseExprKind

## Location
[src/include/parser/parse_node.h:85-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_node.h#L85-L189)

## Overview
ParseExprKind is an enumeration that identifies different contexts in which expressions are being transformed during SQL query parsing, enabling context-specific error messages and validation rules.

## Definition


## Detailed Description
ParseExprKind is a fundamental enumeration used throughout PostgreSQL's parser to distinguish different contexts where expressions are being transformed. While many of these contexts are not semantically distinct for expression transformation purposes, they are distinguished to enable the parser to generate context-specific error messages that are more meaningful to users.

The enum serves as a parameter to  and is stored in the ParseState structure to track the current expression context during recursive parsing. This allows the parser to apply different validation rules and generate appropriate error messages based on where an expression appears in the SQL statement.

The design philosophy behind ParseExprKind is to provide better user experience through more precise error reporting. For example, an aggregate function might be valid in a SELECT target list but invalid in a WHERE clause, and the parser can provide a more helpful error message by knowing which context it's in.

## Parameters / Member Variables
- : Default/invalid state indicating no expression context
- : Reserved for extension code that needs to call transformExpr()
- : Expression in JOIN ON clause
- : Expression in JOIN USING clause  
- : Sub-SELECT expression in FROM clause
- : Function expression in FROM clause
- : Expression in WHERE clause
- : Expression in HAVING clause
- : Expression in aggregate FILTER clause
- : Expression in window PARTITION BY clause
- : Expression in window ORDER BY clause
- : Expression in window frame RANGE clause
- : Expression in window frame ROWS clause
- : Expression in window frame GROUPS clause
- : Expression in SELECT target list
- : Expression in INSERT target list
- : Source expression in UPDATE assignment
- : Target expression in UPDATE assignment
- : Condition in MERGE WHEN [NOT] MATCHED clause
- : Expression in GROUP BY clause
- : Expression in ORDER BY clause
- : Expression in DISTINCT ON clause
- : Expression in LIMIT clause
- : Expression in OFFSET clause
- : Expression in RETURNING clause (INSERT/UPDATE/DELETE)
- : Expression in RETURNING clause for MERGE
- : Expression in VALUES clause
- : Single-row VALUES expression (INSERT only)
- : Expression in table CHECK constraint
- : Expression in domain CHECK constraint
- : Default value expression for table column
- : Default parameter value for function
- : Expression in index definition
- : Predicate in partial index
- : Expression in extended statistics
- : Transform expression in ALTER COLUMN TYPE
- : Parameter value in EXECUTE statement
- : WHEN condition in CREATE TRIGGER
- : USING or WITH CHECK expression in row security policy
- : Expression in partition bound specification
- : Expression in PARTITION BY clause
- : Procedure argument in CALL statement
- : WHERE condition in COPY FROM
- : Generation expression for computed column
- : Cycle mark value in recursive queries

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enum type definition)
- Called from (representative examples):
  -  (src/backend/parser/parse_expr.c:120)
  -  (src/backend/parser/parse_expr.c:3121)
  -  (src/backend/parser/parse_clause.c:1855)
  -  (src/backend/parser/parse_target.c:78)
  -  (src/backend/parser/parse_target.c:122)

## Notes and Other Information
- The enum is designed without a default case in switch statements to ensure compiler warnings when new values are added
-  is specifically reserved for extension code and has no core enforcement of context-driven restrictions
- The parser uses this enum to track expression context in the  field of ParseState
- Context tracking enables better error messages by identifying where problematic expressions occur in SQL statements
- Many enum values map to the same user-facing error message strings in  for simplicity