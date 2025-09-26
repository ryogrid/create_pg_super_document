# InsertStmt

## Location
[src/include/nodes/parsenodes.h:2039-2049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2039-L2049)

## Overview
InsertStmt represents the parsed form of SQL INSERT statements, containing all components needed to specify data insertion into a table.

## Definition
```c
typedef struct InsertStmt
{
    NodeTag             type;
    RangeVar           *relation;           /* relation to insert into */
    List               *cols;               /* optional: names of the target columns */
    Node               *selectStmt;         /* the source SELECT/VALUES, or NULL */
    OnConflictClause   *onConflictClause;   /* ON CONFLICT clause */
    List               *returningList;      /* list of expressions to return */
    WithClause         *withClause;         /* WITH clause */
    OverridingKind      override;           /* OVERRIDING clause */
} InsertStmt;
```

## Detailed Description
InsertStmt represents the complete parsed structure of SQL INSERT statements before transformation into execution plans. It supports all INSERT variants including INSERT ... VALUES, INSERT ... SELECT, and INSERT ... DEFAULT VALUES. The structure accommodates modern PostgreSQL features like conflict resolution (UPSERT), common table expressions (WITH), result returning, and identity column overriding. The source data can come from explicit VALUES lists, SELECT statements, or default values, with the selectStmt field being NULL for DEFAULT VALUES insertions.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an InsertStmt node
- `relation`: Pointer to RangeVar specifying the target table for insertion
- `cols`: List of column names specifying which columns to insert into (NULL means all columns)
- `selectStmt`: Node containing the data source (SelectStmt for VALUES/SELECT, NULL for DEFAULT VALUES)
- `onConflictClause`: Pointer to OnConflictClause specifying conflict resolution behavior (UPSERT)
- `returningList`: List of expressions to return from inserted rows (RETURNING clause)
- `withClause`: Pointer to WithClause containing common table expressions (CTEs)
- `override`: OverridingKind enum specifying identity column override behavior

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar
  - OnConflictClause
  - WithClause
  - OverridingKind
- Called from (representative examples):
  - transformStmt
  - transformInsertStmt
  - transformWithClause
  - makeDependencyGraphWalker
  - raw_expression_tree_walker_impl

## Notes and Other Information
- Part of the optimizable statement category, meaning it undergoes complex parse analysis
- The selectStmt field uses SelectStmt for both VALUES and SELECT data sources for parser uniformity
- Supports PostgreSQL's UPSERT functionality through the onConflictClause field
- The override field handles OVERRIDING SYSTEM VALUE / OVERRIDING USER VALUE for identity columns
- RETURNING clause support enables INSERT statements to return data from inserted rows
- WITH clause support allows complex data transformations before insertion
- Used extensively in data manipulation and ETL operations