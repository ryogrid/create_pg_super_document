# get_range_nulltest

## Location
[src/backend/partitioning/partbounds.c:4676-4721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L4676-L4721)

## Overview
Generates IS NOT NULL expressions for all partition key columns in range partitions, since range partitions do not allow NULL values in partition keys.

## Definition
static List *get_range_nulltest(PartitionKey key)

## Detailed Description
This function creates a list of NullTest expressions that check for non-null values in all partition key columns. Since non-default range partition tables in PostgreSQL do not allow partition keys to be NULL, this function generates the necessary IS NOT NULL constraints for each key column.

The function iterates through all partition key columns, creating appropriate expressions for both attribute-based and expression-based partition keys. Each column gets a corresponding NullTest node configured to test for IS NOT NULL, ensuring that the partition constraint properly excludes NULL values from range partitions.

This is a critical component of range partition constraint generation, as it enforces PostgreSQLs semantic requirement that range partition keys cannot be NULL.

## Parameters / Member Variables
- key: The partition key structure containing information about all partition key columns

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md)
  - [makeVar](../m/makeVar.md)
  - copyObject
  - [lnext](../l/lnext.md)
  - makeNode
- Called from (representative examples):
  - [get_qual_for_range](get_qual_for_range.md)
  - compare_range_bounds

## Notes and Other Information
- Non-default range partitions do not allow NULL partition keys in PostgreSQL
- Creates IS NOT NULL tests for all partition key columns
- Handles both attribute-based partition keys (using makeVar) and expression-based keys (using copyObject)
- Each NullTest node is configured with IS_NOT_NULL type and argisrow = false
- Essential for maintaining PostgreSQLs range partitioning semantics
- The generated constraints are typically ANDed with other range partition constraints
- Error checking ensures the correct number of partition key expressions