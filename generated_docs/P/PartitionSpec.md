# PartitionSpec

## Location
src/include/nodes/parsenodes.h: 882 - 888

## Overview
PartitionSpec is a parse-time representation of a partition key specification that represents the key space used for table partitioning in PostgreSQL.

## Definition


## Detailed Description
PartitionSpec serves as the intermediate representation during SQL parsing phase for partition key specifications. When a CREATE TABLE statement includes a PARTITION BY clause, the parser creates a PartitionSpec structure to capture the partitioning strategy and the list of partitioning columns or expressions. This structure is used during the table creation process to set up the actual partitioning metadata in the system catalogs.

The structure encapsulates all the necessary information needed to define how a table should be partitioned, including the partitioning method (range, list, or hash) and the specific columns or expressions that form the partition key.

## Parameters / Member Variables
- : Standard NodeTag for the PostgreSQL node system, enabling type identification and node traversal
- : The partitioning strategy (PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE, or PARTITION_STRATEGY_HASH)
- : A list of PartitionElem structures representing the partition key columns or expressions
- : Parse location in the original SQL text for error reporting purposes, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - PartitionStrategy
  - ParseLoc
  - NodeTag (inherited)
  - List (PostgreSQL list type)
- Called from (representative examples):
  - transformPartitionSpec
  - CreateStmt
  - RangeVarCallbackForAlterRelation

## Notes and Other Information
- This is a parse-time only structure that gets transformed into catalog entries during table creation
- The partParams list contains PartitionElem nodes that specify individual partition key components
- Used exclusively during DDL processing and is not present in the runtime execution structures
- Location information is crucial for providing meaningful error messages when partition specifications are invalid