# transformRuleStmt

## Location
src/backend/parser/parse_utilcmd.c: 2967 - 3272

## Overview
Transforms a CREATE RULE statement by parsing and analyzing the rule actions and WHERE clause, setting up proper OLD/NEW tuple references, and validating rule constraints.

## Definition
void transformRuleStmt(RuleStmt *stmt, const char *queryString, List **actions, Node **whereClause)

## Detailed Description
The transformRuleStmt function performs comprehensive transformation of CREATE RULE statements, which are fundamental to PostgreSQL's rule system for query rewriting. Its responsibilities include:

1. **Lock Management**: Acquires AccessExclusiveLock on the target relation to prevent deadlocks and ensure consistency during rule definition.

2. **Validation**: Ensures rules are not created on materialized views, which are not supported.

3. **OLD/NEW Setup**: Creates ParseNamespaceItems for OLD and NEW tuple references with specific varno assignments (OLD=1, NEW=2), which are essential for rule processing.

4. **Context-Aware Namespace Management**: Adds OLD and/NEW to the namespace based on the rule event type:
   - SELECT rules: Only OLD is relevant
   - UPDATE rules: Both OLD and NEW are available
   - INSERT rules: Only NEW is relevant  
   - DELETE rules: Only OLD is relevant

5. **WHERE Clause Processing**: Transforms and validates the rule's WHERE clause, ensuring proper collation assignment.

6. **Action Processing**: For non-empty rule actions, it:
   - Creates separate parse states for each action
   - Sets up OLD/NEW references in each sub-query
   - Transforms statements using transformStmt
   - Validates proper OLD/NEW usage based on event type
   - Handles special cases like INSERT...SELECT statements
   - Prevents OLD/NEW usage in WITH queries (CTEs)
   - Manages jointree construction for efficient execution

7. **Special Case Handling**: For 'INSTEAD NOTHING' rules, creates a special CMD_NOTHING query for the rewrite system.

The function includes extensive validation to ensure rules follow PostgreSQL's constraints, such as preventing conditional utility statements and unsupported set operations.

## Parameters / Member Variables
- : RuleStmt structure containing the parsed rule definition to be transformed
- : Original SQL query string used for error reporting and parse state context
- : Output parameter that receives the list of transformed query trees for rule actions
- : Output parameter that receives the transformed WHERE clause node

## Dependencies
- Functions called/Symbols referenced:
  - table_openrv
  - make_parsestate
  - addRangeTableEntryForRelation
  - makeAlias
  - addNSItemToQuery
  - transformWhereClause
  - assign_expr_collations
  - makeFromExpr
  - transformStmt
  - getInsertSelectQuery
  - rangeTableEntry_used
  - free_parsestate
  - table_close
- Called from (representative examples):
  - DefineRule

## Notes and Other Information
- Critical for PostgreSQL's rule system which enables view implementations and query rewriting
- OLD and NEW have fixed varno assignments (1 and 2 respectively) that must be maintained throughout the system
- AccessExclusiveLock prevents deadlocks but limits concurrency during rule creation
- Rules on materialized views are explicitly forbidden due to implementation constraints
- The function handles complex validation scenarios including set operations, CTE usage, and event-specific OLD/NEW constraints
- For efficiency, OLD is only added to the jointree when actually referenced in the rule
- NEW is treated specially in UPDATE rules as a transformed reference to OLD rather than a separate relation
- Extensive error checking ensures rule definitions comply with PostgreSQL's rule system constraints
- The transformation is essential for the rewrite system to properly apply rules during query execution