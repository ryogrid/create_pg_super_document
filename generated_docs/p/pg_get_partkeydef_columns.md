# pg_get_partkeydef_columns

## Location
[src/backend/utils/adt/ruleutils.c:1904-1916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L1904-L1916)

## Overview
Returns a string representation of the partition key column definitions for a partitioned table, focusing on just the column information without additional formatting.

## Definition


## Detailed Description
This function is an internal version of the partition key definition retrieval that specifically reports only the column definitions portion of a partition key. It serves as a wrapper around  with specific parameters to return just the column information. The function is used when you need to extract only the column names and expressions that make up a partition key, without the full SQL syntax that would normally include the "PARTITION BY" clause.

## Parameters / Member Variables
- : Object identifier (OID) of the partitioned relation whose partition key columns should be retrieved
- : Boolean flag indicating whether to use pretty-printing format for the output

## Dependencies
- Functions called/Symbols referenced:
  - GET_PRETTY_FLAGS (macro to convert boolean to formatting flags)
  - [pg_get_partkeydef_worker](pg_get_partkeydef_worker.md) (core worker function that handles partition key definition generation)
- Called from (representative examples):
  - [ExecBuildSlotPartitionKeyDescription](../E/ExecBuildSlotPartitionKeyDescription.md) (in executor for building partition key descriptions)
  - RULE_INDEXDEF_KEYS_ONLY (constant definition in ruleutils.h)

## Notes and Other Information
- This is an internal function that provides a simplified interface to the more general 
- The function calls the worker with parameters  where the third parameter (true) indicates columns-only mode and the fourth parameter (false) indicates not to include the full partition clause
- Used primarily for diagnostic and informational purposes when only the column information is needed, rather than the full partition definition syntax