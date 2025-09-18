# validateInfiniteBounds

## Location
src/backend/parser/parse_utilcmd.c: 4256 - 4294

## Overview
Validates that MAXVALUE or MINVALUE specifications in partition bounds are followed only by more of the same type, ensuring consistency in partition range definitions.

## Definition


## Detailed Description
This function enforces the PostgreSQL partitioning constraint that once a MAXVALUE or MINVALUE is encountered in a partition bound specification, all subsequent bounds must be of the same infinite type. It iterates through a list of partition range bounds and validates that the sequence is consistent - preventing mixed infinite bound types which would create invalid partition definitions.

The function implements a state machine approach where it tracks the current bound type and ensures transitions are valid. Normal values can transition to any type, but once MAXVALUE or MINVALUE is encountered, all following bounds must match that infinite type.

## Parameters / Member Variables
- : ParseState pointer used for error reporting and parser context information
- : List of PartitionRangeDatum elements representing the partition bounds to validate

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md) (enum type)
  - PARTITION_RANGE_DATUM_VALUE (enum constant)
  - PARTITION_RANGE_DATUM_MAXVALUE (enum constant) 
  - PARTITION_RANGE_DATUM_MINVALUE (enum constant)
  - [PartitionRangeDatum](../P/PartitionRangeDatum.md) (struct type)
  - [exprLocation](../e/exprLocation.md) (for error position reporting)
- Called from (representative examples):
  - [transformPartitionRangeBounds](../t/transformPartitionRangeBounds.md)

## Notes and Other Information
- This is a static function within parse_utilcmd.c, used internally for partition bound validation
- Throws ERRCODE_DATATYPE_MISMATCH errors when invalid bound sequences are detected
- Critical for maintaining partition definition integrity in PostgreSQL's declarative partitioning system
- The validation prevents logically impossible partition ranges that could cause runtime errors or incorrect query planning