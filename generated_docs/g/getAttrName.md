# getAttrName

## Location
src/bin/pg_dump/pg_dump.c: 16937 - 16965

## Overview
Retrieves the correct name for a table attribute by attribute number, handling both user-defined columns and PostgreSQL system attributes.

## Definition


## Detailed Description
This utility function provides a unified interface for obtaining attribute names from both user-defined and system-defined columns. Since the TableInfo structure only stores user attribute names in the attnames array, the function must handle system attributes (which have negative or zero attribute numbers) by mapping them to their well-known names.

The function performs bounds checking to ensure the requested attribute number is valid for the given table. For user attributes (positive numbers), it accesses the attnames array using 1-based indexing. For system attributes, it uses a switch statement to map standard PostgreSQL system attribute numbers to their corresponding names like "ctid", "xmin", "xmax", etc.

If an invalid attribute number is provided, the function terminates the program with a fatal error message.

## Parameters / Member Variables
- : Attribute number (positive for user columns, negative/zero for system columns)
- : Table information structure containing user attribute names and total attribute count

## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md)
- Constants referenced:
  - SelfItemPointerAttributeNumber (ctid)
  - MinTransactionIdAttributeNumber (xmin)
  - MinCommandIdAttributeNumber (cmin)
  - MaxTransactionIdAttributeNumber (xmax)
  - MaxCommandIdAttributeNumber (cmax)
  - TableOidAttributeNumber (tableoid)
- Types referenced:
  - [TableInfo](../T/TableInfo.md)
- Called from:
  - [dumpTableSecLabel](../d/dumpTableSecLabel.md)
  - [dumpConstraint](../d/dumpConstraint.md)

## Notes and Other Information
- Uses 1-based indexing for user attributes (attrnum - 1) to access zero-based attnames array
- Handles all standard PostgreSQL system attributes with their conventional names
- Provides bounds checking for safety and debugging
- Returns string literals for system attributes, ensuring consistent naming
- Fatal error handling ensures invalid attribute numbers are caught immediately
- Essential for constraint and security label dumping that may reference system columns