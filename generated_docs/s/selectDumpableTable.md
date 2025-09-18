# selectDumpableTable

## Location
src/bin/pg_dump/pg_dump.c: 1870 - 1908

## Overview
Policy-setting function that determines whether a table should be dumped based on extension membership, inclusion/exclusion lists, and parent namespace dump settings.

## Definition
```c
static void selectDumpableTable(TableInfo *tbinfo, Archive *fout)
```

## Detailed Description
This function implements the dump decision logic for individual tables following a clear hierarchy of rules:

1. **Extension membership check**: First checks if the table belongs to an extension via checkExtensionMembership(). If it does, extension rules override all other considerations and the function returns early.

2. **Table inclusion lists**: If specific tables are being dumped (table_include_oids is not empty), only tables explicitly listed are dumped (DUMP_COMPONENT_ALL), while others get DUMP_COMPONENT_NONE.

3. **Namespace inheritance**: If no specific table list exists, the table inherits its dump setting from its parent namespace's dump_contains flag, allowing schema-level dump decisions to control table dumping.

4. **Exclusion override**: Finally, if the table is explicitly listed in table_exclude_oids, it is excluded regardless of previous decisions (set to DUMP_COMPONENT_NONE).

This hierarchical approach ensures that extension membership takes precedence, followed by explicit inclusion, then namespace-level decisions, with exclusion as the final override.

## Parameters / Member Variables
- `tbinfo`: Pointer to TableInfo structure containing table information and dump flags to be set
- `fout`: Pointer to Archive structure passed to checkExtensionMembership for extension processing

## Dependencies
- Functions called/Symbols referenced:
  - [checkExtensionMembership](../c/checkExtensionMembership.md) (check if table belongs to extension)
  - [simple_oid_list_member](simple_oid_list_member.md) (check membership in include/exclude lists)
  - DUMP_COMPONENT_ALL, DUMP_COMPONENT_NONE (dump component constants)
- Called from (representative examples):
  - [getTables](../g/getTables.md)

## Notes and Other Information
- This is a static function within pg_dump.c used during the dump planning phase
- Extension membership is checked first and takes absolute precedence over all other rules
- The function supports both positive (include) and negative (exclude) table filtering
- Tables inherit dump behavior from their containing namespace unless explicitly overridden
- The function only sets the table's own dump flag, not any subsidiary object dump flags
- Used in conjunction with namespace-level dump decisions to create a complete dump plan