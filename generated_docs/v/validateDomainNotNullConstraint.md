# validateDomainNotNullConstraint

## Location
[src/backend/commands/typecmds.c:3136-3200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3136-L3200)

## Overview
Validates that all columns currently using a domain type contain no null values, ensuring compliance with a NOT NULL constraint being added to the domain.

## Definition


## Detailed Description
This function verifies that existing data in all tables using the specified domain type does not violate a NOT NULL constraint. It retrieves all relations containing columns of the domain type, scans each relation's tuples, and checks that domain-typed columns contain no null values. If any null values are found, it raises an error with detailed information about the violating column and table. The function uses proper snapshot isolation and maintains appropriate locks during validation.

## Parameters / Member Variables
- : Object ID of the domain type to validate for NOT NULL compliance

## Dependencies
- Functions called/Symbols referenced:
  - [get_rels_with_domain](../g/get_rels_with_domain.md)
  - RegisterSnapshot
  - GetLatestSnapshot
  - [table_beginscan](../t/table_beginscan.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - slot_attisnull
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [table_endscan](../t/table_endscan.md)
  - UnregisterSnapshot
  - TupleDescAttr
  - RelationGetRelationName
  - [errtablecol](../e/errtablecol.md)
- Called from (representative examples):
  - [AlterDomainNotNull](../A/AlterDomainNotNull.md)
  - [AlterDomainAddConstraint](../A/AlterDomainAddConstraint.md)

## Notes and Other Information
- Uses ShareLock to prevent concurrent data changes during validation
- Scans all tuples in relations containing domain-typed columns
- Provides detailed error reporting including table and column names for null violations
- Properly manages table scans, snapshots, and tuple slots with cleanup
- Uses the latest snapshot for consistent validation results
- Maintains relation locks after processing to ensure consistency
- Error reporting focuses on table/column information rather than domain type for better user experience