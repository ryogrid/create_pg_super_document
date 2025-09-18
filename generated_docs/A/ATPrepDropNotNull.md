# ATPrepDropNotNull

## Location
src/backend/commands/tablecmds.c: 7532 - 7555

## Overview
Performs preparation and validation for the ALTER TABLE ALTER COLUMN DROP NOT NULL command, specifically handling partitioned table restrictions.

## Definition
```c
static void ATPrepDropNotNull(Relation rel, bool recurse, bool recursing)
```

## Detailed Description
This function is part of PostgreSQL's ALTER TABLE infrastructure and handles the preparation phase for dropping NOT NULL constraints from columns. Its primary responsibility is to validate that the operation is allowed, particularly for partitioned tables where special restrictions apply.

The function enforces a key restriction: NOT NULL constraints cannot be removed from only the parent partitioned table when child partitions exist, unless the operation is set to recurse to all partitions. This maintains consistency across the partition hierarchy and prevents situations where the parent table would allow NULL values while child partitions still enforce NOT NULL.

## Parameters / Member Variables
- `rel`: The relation (table) being altered
- `recurse`: Whether the operation should apply to all partitions in the hierarchy
- `recursing`: Whether this call is part of a recursive operation on child partitions

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md) (retrieves partition descriptor for the relation)
  - [PartitionDesc](../P/PartitionDesc.md) (type for partition information)
  - ereport (for error reporting)
  - [errmsg](../e/errmsg.md)/errhint (for error message formatting)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (main ALTER TABLE command preparation dispatcher)

## Notes and Other Information
- The function is static, meaning it's only used within tablecmds.c
- Part of the ALTER TABLE command processing pipeline (preparation phase)
- Specifically handles partitioned table constraints to maintain hierarchy consistency
- The error suggests using RECURSE instead of ONLY keyword for partitioned tables
- Does not perform the actual constraint removal - that's handled in the execution phase
- The check only applies to partitioned tables (RELKIND_PARTITIONED_TABLE)
- Uses Assert to ensure partition descriptor is valid when dealing with partitioned tables