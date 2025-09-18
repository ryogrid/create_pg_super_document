# stmt_requires_parse_analysis

## Location
[src/backend/parser/analyze.c:441-484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L441-L484)

## Overview
Determines whether a raw statement requires non-trivial parse analysis or can be treated as a simple utility command.

## Definition
```c
bool stmt_requires_parse_analysis(RawStmt *parseTree)
```

## Detailed Description
This function serves as a classifier that determines whether a given raw statement needs complex semantic analysis and transformation, or whether it can be handled as a simple utility command. The function is crucial for optimization decisions throughout PostgreSQL's execution pipeline.

The function returns true for statement types that require:
- Complex semantic analysis and transformation
- Query optimization and planning
- Rewriting and rule application

It returns false for utility statements that are simply wrapped in CMD_UTILITY Query nodes without further processing. This classification helps the system avoid unnecessary reprocessing and enables various optimization strategies.

The function categorizes statements into three groups:
1. **Optimizable statements** - DML operations and procedural statements that undergo full transformation
2. **Special cases** - Statements that require specialized processing but aren't traditional DML
3. **Utility statements** - All other statements that require minimal processing

## Parameters / Member Variables
- : RawStmt structure containing the raw parse tree node to be analyzed

## Dependencies
- Functions called/Symbols referenced:
  - RawStmt (structure access for statement extraction)
  - nodeTag (node type identification)

- Called from (representative examples):
  - [analyze_requires_snapshot](../a/analyze_requires_snapshot.md) (determining snapshot requirements)
  - StmtPlanRequiresRevalidation (plan cache revalidation logic)

## Notes and Other Information
- The function is tightly coordinated with transformStmt() - it should return true for any statement type where transformStmt() does more than simple CMD_UTILITY wrapping
- Currently, a false result indicates that the entire parse analysis/rewrite/plan pipeline will never need re-execution, though this assumption may change in future versions
- The function is essential for plan caching and revalidation strategies
- Statement types handled as 'optimizable' include INSERT, DELETE, UPDATE, MERGE, SELECT, RETURN, and PL assignment statements
- 'Special case' statements like DECLARE CURSOR, EXPLAIN, CREATE TABLE AS, and CALL also require parse analysis despite not being traditional DML