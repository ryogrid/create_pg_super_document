# simple_oid_list_member

## Location
src/fe_utils/simple_list.c: 45 - 62

## Overview
Checks whether a specific OID (Object Identifier) value exists in a simple linked list structure designed for frontend PostgreSQL utilities.

## Definition
```c
bool simple_oid_list_member(SimpleOidList *list, Oid val)
```

## Detailed Description
This function performs a linear search through a SimpleOidList to determine if a given OID value is present in the list. It iterates through each cell in the linked list, comparing the stored value with the target value. The function returns true if the OID is found, false otherwise. This is part of the simple list facilities designed for frontend code like pg_dump, providing basic membership testing functionality without the complexity of the backend's List infrastructure.

## Parameters / Member Variables
- `list`: Pointer to the SimpleOidList structure to search in
- `val`: The OID value to search for in the list

## Dependencies
- Data structures used:
  - [SimpleOidList](../S/SimpleOidList.md) (the list container structure)
  - [SimpleOidListCell](../S/SimpleOidListCell.md) (individual list node structure)
- Called from (representative examples):
  - [selectDumpableNamespace](selectDumpableNamespace.md) (src/bin/pg_dump/pg_dump.c:1802, 1851)
  - [selectDumpableTable](selectDumpableTable.md) (src/bin/pg_dump/pg_dump.c:1880, 1890)
  - [selectDumpableExtension](selectDumpableExtension.md) (src/bin/pg_dump/pg_dump.c:2083, 2093)
  - [makeTableDataInfo](../m/makeTableDataInfo.md) (src/bin/pg_dump/pg_dump.c:2842, 2855)
  - [processExtensionTables](../p/processExtensionTables.md) (src/bin/pg_dump/pg_dump.c:18405, 18414, 18448, 18460, 18465)

## Notes and Other Information
- Performs O(n) linear search through the list
- Returns false immediately if an empty list is provided
- No memory allocation is performed during the search operation
- This is specifically designed for frontend utilities and is simpler than backend List facilities
- The function is commonly used in pg_dump for checking if specific database objects (schemas, tables, extensions) should be included in dump operations