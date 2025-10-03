# specialAttNum

## Location
[src/backend/parser/parse_relation.c:3514-3532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3514-L3532)

## Overview
This static function checks if an attribute name corresponds to a PostgreSQL system attribute and returns its attribute number.

## Definition

```c
static int
specialAttNum(const char *attname)
```
## Detailed Description
The  function determines whether a given attribute name is a special system attribute (such as "xmin", "xmax", "ctid", etc.) and returns the corresponding attribute number if found. It uses  to look up the attribute name in the system attribute catalog. This function only identifies potential system attributes by name - the caller is responsible for ensuring that the attribute actually exists in the target relation. System attributes have negative attribute numbers to distinguish them from regular user-defined attributes.

## Parameters / Member Variables
- `*attname`: The name of the attribute to check for being a system attribute
## Dependencies
- Functions called/Symbols referenced:
  - [SystemAttributeByName](../S/SystemAttributeByName.md)
  - InvalidAttrNumber
  - FormData_pg_attribute
- Called from (representative examples):
  - [attnameAttNum](../a/attnameAttNum.md)
  - [scanRTEForColumn](scanRTEForColumn.md)

## Notes and Other Information
- This is a static function, only accessible within parse_relation.c
- Returns InvalidAttrNumber if the attribute name is not a system attribute
- System attributes have negative attribute numbers (e.g., ctid = -1, xmin = -4)
- The function only validates the name pattern, not whether the attribute exists in a specific relation
- Added by Thomas Lockhart in 2000 to support system attribute recognition

## Simplified Source

```c
static int
specialAttNum(const char *attname)
{
    const FormData_pg_attribute *sysatt;

    // Look up system attribute by name
    sysatt = SystemAttributeByName(attname);

    if (sysatt != NULL)
        return sysatt->attnum;  // Return system attribute number

    return InvalidAttrNumber;   // Not a system attribute
}
```