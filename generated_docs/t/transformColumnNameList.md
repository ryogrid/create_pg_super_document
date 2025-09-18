# transformColumnNameList

## Location
[src/backend/commands/tablecmds.c:11893-11944](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11893-L11944)

## Overview
transformColumnNameList transforms a list of column names into their corresponding attribute numbers and optionally their type OIDs, specifically designed for foreign key constraint processing.

## Definition


## Detailed Description
This function takes a list of column names and resolves them to their internal attribute numbers and type OIDs for a given relation. It performs several validation checks to ensure the columns are suitable for use in foreign key constraints:

1. Verifies each column exists in the relation
2. Ensures columns are not system columns (attnum >= 0) 
3. Enforces the INDEX_MAX_KEYS limit on the number of columns
4. Populates arrays with attribute numbers and optionally type OIDs

The function is specifically tailored for foreign key processing, as evidenced by its error messages and validation logic. It returns the total number of columns processed.

## Parameters / Member Variables
- : OID of the relation to look up columns in
- : List of column names (as String nodes) to transform
- : Output array to store attribute numbers (must be pre-allocated)
- : Optional output array to store column type OIDs (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strVal
  - lfirst
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - INDEX_MAX_KEYS (constant)
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md) (for both referencing and referenced column lists)

## Notes and Other Information
- Despite its general-purpose name, this function is specifically designed for foreign key processing
- Error messages are tailored to foreign key contexts and would need modification for other uses
- System columns (negative attribute numbers) are explicitly prohibited
- The function enforces PostgreSQL's limit of INDEX_MAX_KEYS columns in a foreign key
- The atttypids parameter is optional and can be NULL if type information isn't needed
- Returns the number of columns processed, which should match the length of the input colList