# find_duplicate_ors

## Location
[src/backend/optimizer/prep/prepqual.c:406-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepqual.c#L406-L516)

## Overview
Searches for OR clauses where the inverse OR distributive law can be applied to extract common terms, and removes NULL constants from top-level AND/OR structures during qualification processing.

## Definition

```c
static Expr *
find_duplicate_ors(Expr *qual, bool is_check)
```
## Detailed Description
The  function applies the inverse OR distributive law to optimize boolean expressions by identifying and extracting common terms from OR clauses. It transforms expressions like  into , reducing redundancy and improving query execution efficiency.

The function operates recursively on the top-level AND/OR structure and performs several key optimizations:

**Inverse OR Distributive Law**: Extracts common conditions from OR clauses, which is particularly beneficial for machine-generated queries and TPC benchmark queries.

**Constant Elimination**: Removes NULL constants from top-level structures with context-specific handling:
- In WHERE clauses: NULL is treated as FALSE, so  becomes 
- In CHECK constraints: NULL may be treated as TRUE, so behavior differs

**Recursive Processing**: Processes nested boolean expressions while preserving AND/OR flatness.

The function handles both OR and AND clauses differently:
- OR clauses: Looks for duplicate terms to extract and simplifies constant expressions
- AND clauses: Flattens nested ANDs and handles constant folding
- Other expressions: Returns unchanged

## Parameters / Member Variables
- : The qualification expression to process (Expr pointer)
- : Boolean flag indicating whether this is a CHECK constraint (true) or WHERE/JOIN clause (false) - affects NULL handling

## Dependencies
- Functions called/Symbols referenced:
  -  - checks if expression is an OR clause
  -  - checks if expression is an AND clause
  -  - recursive calls for nested processing
  -  - creates boolean constant nodes
  -  - flattens nested OR clauses
  -  - flattens nested AND clauses
  -  - performs the actual duplicate OR processing
  -  - creates AND expression nodes
  -  - boolean expression node type
- Called from (representative examples):
  -  (src/backend/optimizer/prep/prepqual.c:309)
  -  (recursive calls) (src/backend/optimizer/prep/prepqual.c:418, 462)

## Notes and Other Information
- This is a static function, only used within the prepqual.c module
- Preserves AND/OR flatness throughout the transformation process
- The inverse OR distributive law optimization was one of the main useful features of the old canonical AND-of-ORs approach
- Different handling of NULL constants depending on context (WHERE vs CHECK constraints)
- Essential for optimizing machine-generated queries that often contain redundant boolean structures
- Returns the original expression unchanged if no optimizations can be applied
- Part of PostgreSQL's boolean expression optimization pipeline in the query planner