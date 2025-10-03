# attnameAttNum

## Location
[src/backend/parser/parse_relation.c:3483-3513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3483-L3513)

## Overview
This function retrieves the attribute number (attnum) for a given attribute name within an opened relation.

## Definition

```c
int
attnameAttNum(Relation rd, const char *attname, bool sysColOK)
```
## Detailed Description
The  function searches through a relation's attributes to find the attribute number corresponding to a given attribute name. It iterates through all regular attributes in the relation and compares their names using . If the attribute is found and not dropped, it returns the 1-based attribute number. If  is true and no regular attribute matches, it also searches system columns using . This function should only be used on relations that are already opened with . For non-opened relations, the cache version  should be used instead.

## Parameters / Member Variables
- `rd`: The opened relation to search within
- `*attname`: The name of the attribute to find
- `sysColOK`: Whether to include system columns in the search if no regular attribute is found
## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - TupleDescAttr
  - [namestrcmp](../n/namestrcmp.md)
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

## Simplified Source

```c
int attnameAttNum(Relation rd, const char *attname, bool sysColOK)
{
    int i;

    // Search through regular attributes
    for (i = 0; i < RelationGetNumberOfAttributes(rd); i++) {
        Form_pg_attribute att = TupleDescAttr(rd->rd_att, i);

        // Check name match and not dropped
        if (namestrcmp(&(att->attname), attname) == 0 && !att->attisdropped)
            return i + 1;  // Return 1-based attribute number
    }

    // Check system columns if allowed
    if (sysColOK) {
        if ((i = specialAttNum(attname)) != InvalidAttrNumber)
            return i;
    }

    // Attribute not found
    return InvalidAttrNumber;
}
```