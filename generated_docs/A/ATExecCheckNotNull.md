# ATExecCheckNotNull

## Location
[src/backend/commands/tablecmds.c:7842-7870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7842-L7870)

## Overview
ATExecCheckNotNull is a validation function that verifies a column already has the NOT NULL constraint set, used primarily for partition validation when ALTER TABLE ONLY ... SET NOT NULL is applied to partitioned tables.

## Definition


## Detailed Description
This function performs a validation-only check for the NOT NULL constraint rather than actually setting it. It serves a specific purpose in PostgreSQL's partitioned table architecture:

1. **Generated Command**: This function handles the AT_CheckNotNull command type, which doesn't exist in SQL grammar but is internally generated when users execute ALTER TABLE ONLY ... SET NOT NULL on partitioned tables.

2. **Partition Validation**: When a partitioned table receives a SET NOT NULL command with the ONLY keyword, the system generates CHECK NOT NULL commands for all partitions to ensure they already comply with the constraint.

3. **Error Reporting**: If any partition doesn't already have the NOT NULL constraint, it reports a specific error message suggesting the user remove the ONLY keyword to allow modification of child tables.

4. **Future Design**: The comments indicate this function may be enhanced in the future to support inheritance count tracking for NOT NULL constraints, but currently only performs validation.

This approach ensures that partitioned tables maintain consistency - if the parent table should be NOT NULL, all partitions must already be NOT NULL.

## Parameters / Member Variables
- : AlteredTableInfo structure (currently unused but part of standard ALTER TABLE interface)
- : The partition or child relation being checked
- : Name of the column to verify has NOT NULL constraint
- : Lock mode for accessing the relation (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (to lookup column in system catalog)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (to release catalog cache entry)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command execution dispatcher)

## Notes and Other Information
- This command type (AT_CheckNotNull) is not directly accessible via SQL grammar but is generated internally by the system
- The function is specifically designed for partitioned table scenarios where ONLY keyword is used with SET NOT NULL
- Provides helpful error messages when partitions don't comply, including a hint to remove the ONLY keyword
- Part of PostgreSQL's strategy to maintain constraint consistency across partition hierarchies without modifying child tables
- Future versions may extend this to support constraint inheritance counting mechanisms