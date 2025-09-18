# shouldPrintColumn

## Location
src/bin/pg_dump/pg_dump.c: 9362 - 9379

## Overview
This function determines whether a specific column should be printed as part of a table's CREATE TABLE statement during PostgreSQL database dumps.

## Definition
bool shouldPrintColumn(const DumpOptions *dopt, const TableInfo *tbinfo, int colno)

## Detailed Description
The shouldPrintColumn function is part of the pg_dump utility and controls which columns are included when generating CREATE TABLE statements. It implements specific logic to handle edge cases:

- **Normal operation**: Returns true for most columns that should appear in the CREATE TABLE statement
- **Dropped columns**: Returns false for columns marked as dropped (attisdropped)
- **Inherited columns**: Returns false for columns inherited without local definition to prevent incorrectly setting pg_attribute.attislocal to true
- **Partitions**: Always returns true for partition tables since they should be created independently before using ATTACH PARTITION
- **Binary upgrade mode**: Always returns true to maintain physical column order, with attislocal/attisdropped state fixed later

This centralized decision function ensures consistency across various parts of pg_dump that need to make the same determination.

## Parameters / Member Variables
- : Pointer to DumpOptions structure containing dump configuration settings, including binary_upgrade flag
- : Pointer to TableInfo structure containing metadata about the table being processed
- : Zero-based column number being evaluated for inclusion

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions (struct)
  - [TableInfo](../T/TableInfo.md) (struct)
- Called from (representative examples):
  - [flagInhAttrs](../f/flagInhAttrs.md)
  - [getTableAttrs](../g/getTableAttrs.md)  
  - [dumpTableSchema](../d/dumpTableSchema.md)

## Notes and Other Information
- The function exists to centralize this decision logic across multiple scattered locations in pg_dump
- In binary upgrade mode, all columns are printed regardless of other conditions to preserve physical column ordering
- The logic carefully balances correctness of pg_attribute flags with the needs of different dump scenarios
- Column numbering is zero-based, consistent with PostgreSQL's internal column numbering