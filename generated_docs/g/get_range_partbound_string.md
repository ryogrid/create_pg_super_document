# get_range_partbound_string

## Location
[src/backend/utils/adt/ruleutils.c:13346-13379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L13346-L13379)

## Overview
A utility function that creates a C string representation of range partition boundary values, formatting them as a parenthesized, comma-separated list suitable for SQL output.

## Definition
```c
char *
get_range_partbound_string(List *bound_datums)
```

## Detailed Description
This function processes a list of PartitionRangeDatum structures representing the boundary values for a range partition and formats them into a human-readable string. The output follows SQL syntax for partition bounds, with values enclosed in parentheses and separated by commas. The function handles special boundary cases like MINVALUE and MAXVALUE, and uses the standard PostgreSQL expression deparsing context to format actual values. This is primarily used when displaying or reconstructing partition boundary definitions in DDL statements.

## Parameters / Member Variables
- `bound_datums`: A List of PartitionRangeDatum structures representing the partition boundary values to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - [makeStringInfo](../m/makeStringInfo.md)
  - memset
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - lfirst_node
  - castNode
  - [get_const_expr](get_const_expr.md)
- Structures/Types referenced:
  - [deparse_context](../d/deparse_context.md)
  - [PartitionRangeDatum](../P/PartitionRangeDatum.md)
  - PARTITION_RANGE_DATUM_MINVALUE
  - PARTITION_RANGE_DATUM_MAXVALUE
  - [Const](../C/Const.md)
- Called from (representative examples):
  - [check_new_partition_bound](../c/check_new_partition_bound.md) (in partbounds.c)
  - [get_rule_expr](get_rule_expr.md)

## Notes and Other Information
- This is a non-static function, making it available to other modules that need to format partition bounds
- Handles three types of partition range datums: MINVALUE, MAXVALUE, and actual constant values
- Uses the standard deparse_context mechanism for consistent expression formatting
- The returned string is allocated in the current memory context and should be managed by the caller
- Output format follows SQL syntax: "(value1, value2, ...)" or "(MINVALUE, value, MAXVALUE)" etc.
- Used in both partition validation and SQL output generation contexts