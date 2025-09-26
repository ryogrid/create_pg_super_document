# binary_upgrade_set_missing_value

## Location
[src/backend/utils/adt/pg_upgrade_support.c:261-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L261-L284)

## Overview
Sets missing values for table attributes during binary upgrades, allowing PostgreSQL to handle columns with default values that were added after the table was created.

## Definition

```c
Datum
binary_upgrade_set_missing_value(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of PostgreSQL's binary upgrade support infrastructure. It enables setting missing values for table attributes during the upgrade process from older PostgreSQL versions. Missing values are used to represent default values for columns that were added to existing tables without requiring a full table rewrite. The function extracts the table OID, attribute name, and value from its arguments, then calls the internal SetAttrMissing function to perform the actual operation.

The function includes a CHECK_IS_BINARY_UPGRADE macro call to ensure it can only be executed during binary upgrade operations, preventing misuse in normal database operations.

## Parameters / Member Variables
- : The object identifier of the target table
- : The name of the attribute/column to set the missing value for  
- : The missing value to be set for the attribute

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - PG_GETARG_TEXT_P
  - [text_to_cstring](../t/text_to_cstring.md)
  - CHECK_IS_BINARY_UPGRADE
  - [SetAttrMissing](../S/SetAttrMissing.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct callers found (likely called via SQL during binary upgrades)

## Notes and Other Information
- This is a PostgreSQL built-in function specifically designed for binary upgrade scenarios
- The function is protected by CHECK_IS_BINARY_UPGRADE to prevent execution outside upgrade contexts
- Missing values are an optimization that avoids rewriting entire tables when adding columns with defaults
- Located in src/backend/utils/adt/pg_upgrade_support.c:261-284