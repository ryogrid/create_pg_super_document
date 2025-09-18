# attnameAttNum

## Location
src/backend/parser/parse_relation.c: 3483 - 3513

## Overview
This function retrieves the attribute number (attnum) for a given attribute name within an opened relation.

## Definition


## Detailed Description
The  function searches through a relation's attributes to find the attribute number corresponding to a given attribute name. It iterates through all regular attributes in the relation and compares their names using . If the attribute is found and not dropped, it returns the 1-based attribute number. If  is true and no regular attribute matches, it also searches system columns using . This function should only be used on relations that are already opened with . For non-opened relations, the cache version  should be used instead.

## Parameters / Member Variables
- : The opened relation to search within
- : The name of the attribute to find
- : Whether to include system columns in the search if no regular attribute is found

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - TupleDescAttr
  - namestrcmp
  - [specialAttNum](../s/specialAttNum.md)
  - InvalidAttrNumber
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)
  - [transformUpdateTargetList](../t/transformUpdateTargetList.md)
  - [checkInsertTargets](../c/checkInsertTargets.md)

## Notes and Other Information
- Returns InvalidAttrNumber if the attribute doesn't exist or is dropped
- Uses 1-based attribute numbering (adds 1 to the internal 0-based index)
- Only works with already opened relations - use get_attnum() for non-opened relations
- Supports searching system columns when sysColOK is true