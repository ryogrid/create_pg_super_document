# PartitionCmd

## Location
src/include/nodes/parsenodes.h: 953 - 959

## Overview
PartitionCmd represents information for ALTER TABLE/INDEX ATTACH/DETACH PARTITION commands, encapsulating the details needed to attach or detach partitions from partitioned tables.

## Definition


## Detailed Description
PartitionCmd is used during ALTER TABLE operations to specify partition attachment and detachment operations. When attaching a partition, it contains the partition bound specification that defines which data belongs to the partition. When detaching, it identifies the partition to be removed. The structure supports both regular and concurrent operations, where concurrent operations allow other transactions to continue working with the partitioned table during the partition management operation.

This command structure is processed by the ALTER TABLE infrastructure and translates into the appropriate catalog updates and constraint validations needed to maintain the partitioning system's integrity.

## Parameters / Member Variables
- : Standard NodeTag for the PostgreSQL node system
- : RangeVar pointer specifying the name of the partition table to attach or detach, including schema qualification if needed
- : PartitionBoundSpec pointer defining the partition bounds when attaching a partition (FOR VALUES clause), NULL when detaching
- : Boolean flag indicating whether the operation should be performed concurrently, allowing other transactions to access the table during the operation

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md)
  - [PartitionBoundSpec](PartitionBoundSpec.md)
  - NodeTag (inherited)
- Called from (representative examples):
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)
  - [transformPartitionCmd](../t/transformPartitionCmd.md)
  - [AlterTableGetLockLevel](../A/AlterTableGetLockLevel.md)
  - [ATExecCmd](../A/ATExecCmd.md)
  - [ProcessUtilitySlow](ProcessUtilitySlow.md)

## Notes and Other Information
- Used exclusively for partition management DDL operations (ATTACH/DETACH PARTITION)
- The bound field is only used for ATTACH PARTITION operations and contains the FOR VALUES specification
- Concurrent operations provide better availability during partition management but may have additional restrictions
- Part of the ALTER TABLE command processing pipeline and integrates with the constraint validation system
- The structure supports both table and index partition operations
- Proper locking and validation are essential to maintain partitioning integrity during these operations