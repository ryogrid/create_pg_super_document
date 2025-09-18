# build_replindex_scan_key

## Location
src/backend/executor/execReplication.c: 96 - 175

## Overview
Constructs a ScanKey array for searching a relation using an index, specifically designed for replication identity operations including primary keys, replica identity, and REPLICA IDENTITY FULL indexes.

## Definition
```c
static int build_replindex_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleTableSlot *searchslot)
```

## Detailed Description
This function builds a scan key that can be used to locate a specific tuple in a relation using an index scan. It is specifically designed for replication scenarios and works with primary keys, replica identity indexes, or indexes suitable for REPLICA IDENTITY FULL tables.

The function iterates through each key attribute of the index, constructing scan key entries for non-expression attributes only. For each valid attribute, it:
1. Retrieves operator class information to determine the appropriate equality operator
2. Gets the equality strategy number for the operator class
3. Finds the corresponding equality operator function
4. Initializes a scan key entry with the appropriate strategy, function, and value
5. Sets proper collation and handles null values with special flags

The function only processes attributes that have valid table attribute numbers, skipping expression-based index columns since they are not currently supported for replication scans.

## Parameters / Member Variables
- `skey`: Output array of ScanKey structures to be populated
- `rel`: The base relation being searched (not the index relation)
- `idxrel`: The index relation to use for the scan
- `searchslot`: TupleTableSlot containing the values to search for

## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (to get index operator classes)
  - IndexRelationGetNumberOfKeyAttributes (to get number of key attributes)
  - AttributeNumberIsValid (to validate attribute numbers)
  - [get_opclass_input_type](../g/get_opclass_input_type.md) (to get operator class input type)
  - [get_opclass_family](../g/get_opclass_family.md) (to get operator class family)
  - [get_equal_strategy_number](../g/get_equal_strategy_number.md) (to get equality strategy number)
  - [get_opfamily_member](../g/get_opfamily_member.md) (to find equality operator)
  - [get_opcode](../g/get_opcode.md) (to get operator function)
  - [ScanKeyInit](../S/ScanKeyInit.md) (to initialize scan key entries)

- Called from (representative examples):
  - [RelationFindReplTupleByIndex](../R/RelationFindReplTupleByIndex.md)

## Notes and Other Information
- This is a static function only accessible within execReplication.c
- Does not support expression-based index columns - only simple column references
- Requires at least one valid attribute to create a scan key (asserted)
- Properly handles null values by setting SK_ISNULL and SK_SEARCHNULL flags
- Sets appropriate collation for each scan key entry from the index definition
- Designed specifically for replication identity scenarios, not general-purpose index scanning
- Returns the number of scan key entries created (skey_attoff)