# btint4sortsupport

## Location
[src/backend/access/nbtree/nbtcompare.c:123-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L123-L131)

## Overview
A PostgreSQL function that sets up sort support for 32-bit integer (int4) data types in B-tree indexes.

## Definition

```c
Datum
btint4sortsupport(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL internal function that configures sort support for 32-bit integer operations in B-tree indexes. It initializes a SortSupport structure by setting the comparator function to , which enables optimized sorting operations for int4 values during index operations and sorting queries.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function parameters through the function call context

## Dependencies
- Functions called/Symbols referenced:
  -  (type/structure)
  -  (comparator function)
  -  (PostgreSQL return macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's B-tree index support infrastructure
- Located in 
- The function follows PostgreSQL's standard pattern for sort support functions by setting up the appropriate comparator for the data type
- Returns void as it only modifies the passed SortSupport structure