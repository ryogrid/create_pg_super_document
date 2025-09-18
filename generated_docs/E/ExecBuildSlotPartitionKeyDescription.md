# ExecBuildSlotPartitionKeyDescription

## Location
src/backend/executor/execPartition.c: 1611 - 1698

## Overview
Builds a human-readable string description of partition key values for error messages when ExecFindPartition() fails to find a suitable partition for a row.

## Definition
```c
static char *ExecBuildSlotPartitionKeyDescription(Relation rel, Datum *values, bool *isnull, int maxfieldlen)
```

## Detailed Description
This function creates a formatted string representation of partition key values, similar to BuildIndexValueDescription(). It is primarily used for generating informative error messages when partition routing fails. The function performs several security and access control checks before building the description:

1. **Row-Level Security Check**: Returns NULL if RLS is enabled to avoid exposing sensitive data
2. **Table-Level Access Check**: Verifies the user has SELECT permissions on the table
3. **Column-Level Access Check**: For each partition key column, ensures the user has SELECT privileges
4. **Expression Handling**: Returns NULL for expression-based partition keys to avoid complex privilege analysis

The output format is "(column1, column2, ...) = (value1, value2, ...)" with proper truncation of long values and handling of NULL values.

## Parameters / Member Variables
- `rel`: Relation object representing the partitioned table
- `values`: Array of Datum values representing the partition key values
- `isnull`: Array of boolean flags indicating which partition key values are NULL  
- `maxfieldlen`: Maximum length allowed for each field value in the output string before truncation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetPartitionKey
  - get_partition_natts
  - check_enable_rls
  - pg_class_aclcheck
  - get_partition_col_attnum
  - pg_attribute_aclcheck
  - pg_get_partkeydef_columns
  - getTypeOutputInfo
  - get_partition_col_typid
  - OidOutputFunctionCall
  - pg_mbcliplen
  - appendBinaryStringInfo
- Called from (representative examples):
  - ExecFindPartition (when generating error messages for partition routing failures)

## Notes and Other Information
- Returns NULL if access control checks fail, preventing information disclosure to unauthorized users
- Handles multibyte character truncation properly using pg_mbcliplen() to avoid splitting characters
- Expression-based partition keys return NULL to avoid complex privilege checking on underlying columns
- NULL values are displayed as "null" in the output string
- Long field values are truncated with "..." suffix to keep error messages manageable
- This is a static function used internally for error reporting in the partition routing subsystem
- The function balances informative error messages with security considerations by respecting PostgreSQL's access control model