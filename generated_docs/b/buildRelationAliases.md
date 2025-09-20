# buildRelationAliases

## Location
[src/backend/parser/parse_relation.c:1177-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1177-L1253)

## Overview
Constructs the eref column name list for a relation RTE (Range Table Entry), handling user-supplied column aliases and dropped columns.

## Definition

```c
static void
buildRelationAliases(TupleDesc tupdesc, Alias *alias, Alias *eref)
```
## Detailed Description
This function builds the effective reference (eref) column name list for a relation or function RTE. It processes the physical column information from a tuple descriptor and applies user-supplied column aliases where provided. The function handles dropped columns by inserting empty strings to maintain proper alignment with physical column numbers. It also rebuilds the alias->colnames list to ensure one-to-one correspondence with physical columns, and validates that the number of user-supplied aliases doesn't exceed the number of available columns.

## Parameters / Member Variables
- : The tuple descriptor containing physical column information for the relation
- : The user-supplied alias structure containing column names, or NULL if no aliases provided
- : The effective reference alias structure where the final column names will be stored

## Dependencies
- Functions called/Symbols referenced:
  - list_head (list manipulation)
  - list_length (list manipulation) 
  - [lnext](../l/lnext.md) (list traversal)
  - lappend (list building)
  - [makeString](../m/makeString.md) (string creation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - ereport (error reporting)
- Called from (representative examples):
  - [addRangeTableEntry](../a/addRangeTableEntry.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - [addRangeTableEntryForENR](../a/addRangeTableEntryForENR.md)

## Notes and Other Information
- The function modifies both the eref->colnames list (output) and rebuilds alias->colnames for consistency
- Dropped columns are handled by inserting empty strings to preserve column position alignment
- Error checking ensures users don't specify more column aliases than available non-dropped columns
- This code is shared between relation and function RTEs for consistent alias handling
- The function operates at parse time during query analysis to establish proper column references