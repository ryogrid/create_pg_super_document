# boot_openrel

## Location
src/backend/bootstrap/bootstrap.c: 408 - 452

## Overview
boot_openrel executes the BKI (Backend Interface) OPEN command during PostgreSQL bootstrap, opening a relation and preparing its attribute information for subsequent bootstrap operations.

## Definition
```c
void boot_openrel(char *relname)
```

## Detailed Description
boot_openrel is a core function in PostgreSQL's bootstrap process that handles the BKI OPEN command. BKI (Backend Interface) is the special bootstrap language used during PostgreSQL initialization to create and populate system catalogs.

The function performs several important operations:
1. **Name validation**: Truncates relation names that exceed NAMEDATALEN to ensure they fit within PostgreSQL's naming constraints
2. **Type system initialization**: Ensures the Typ list is populated by calling populate_typ_list() if not already done, as pg_type must be available before opening relations
3. **Relation cleanup**: Closes any previously opened relation to maintain proper state
4. **Relation opening**: Uses the table access method to open the specified relation with no locking (NoLock)
5. **Attribute setup**: Copies attribute information from the relation's tuple descriptor into the global attrtypes array for use by subsequent bootstrap commands

This function is essential for setting up the context needed to process INSERT commands and other bootstrap operations on the opened relation.

## Parameters / Member Variables
- `relname`: Name of the relation to open (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - populate_typ_list
  - closerel 
  - table_openrv
  - makeRangeVar
  - RelationGetNumberOfAttributes
  - AllocateAttribute
  - TupleDescAttr
  - elog (for debug output)
- Called from (representative examples):
  - BKI parser/grammar (referenced in bootstrap header)

## Notes and Other Information
- Part of the "MANUAL BACKEND INTERACTIVE INTERFACE COMMANDS" section
- Uses global variables: boot_reldesc, numattr, attrtypes, Typ
- Truncates relation names to NAMEDATALEN-1 characters to ensure null termination
- Opens relations with NoLock since bootstrap runs in single-user mode
- Copies ATTRIBUTE_FIXED_PART_SIZE bytes for each attribute descriptor
- Includes DEBUG4 logging for relation opening and attribute creation details
- The function assumes pg_type is already populated, which is ensured by calling populate_typ_list()
- Located in src/backend/bootstrap/bootstrap.c:408-452
- Works in conjunction with closerel() to manage relation state during bootstrap