# makeRangeVar

## Location
[src/backend/nodes/makefuncs.c:471-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L471-L492)

## Overview
Creates a RangeVar node representing a table/relation reference with schema and relation names.

## Definition
```c
RangeVar *makeRangeVar(char *schemaname, char *relname, int location)
```

## Detailed Description
The `makeRangeVar` function creates a RangeVar node that represents a reference to a database relation (table, view, etc.). This is described as a "rather oversimplified case" in the comment, as it sets several fields to default values. The function initializes a RangeVar with the provided schema and relation names, sets inheritance to true by default, assumes permanent table persistence, and sets no alias or catalog name.

## Parameters / Member Variables
- `schemaname`: The name of the schema containing the relation (can be NULL for current schema)
- `relname`: The name of the relation being referenced
- `location`: Source location in the original query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [RangeVar](../R/RangeVar.md) (struct type)
  - RELPERSISTENCE_PERMANENT (constant)
- Called from (representative examples):
  - [makeRangeVarFromNameList](makeRangeVarFromNameList.md)
  - [DoCopy](../D/DoCopy.md)
  - [LookupTypeNameExtended](../L/LookupTypeNameExtended.md)
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md)
  - [autovacuum_do_vac_analyze](../a/autovacuum_do_vac_analyze.md)

## Notes and Other Information
- Sets catalogname to NULL (no cross-database references)
- Sets inh to true by default (inheritance is enabled)
- Sets relpersistence to RELPERSISTENCE_PERMANENT (assumes permanent tables)
- Sets alias to NULL (no table alias)
- This is a simplified constructor - more complex RangeVar nodes may need additional configuration
- Used throughout PostgreSQL for representing table/relation references in various contexts
- Essential for parsing and processing SQL statements that reference database objects