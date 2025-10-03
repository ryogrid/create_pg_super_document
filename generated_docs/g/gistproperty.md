# gistproperty

## Location
[src/backend/access/gist/gistutil.c:932-1014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L932-L1014)

## Overview
Checks boolean properties of GiST (Generalized Search Tree) indexes, particularly for properties not supported by the core property code like distance orderability and data returnability.

## Definition

```c
bool
gistproperty(Oid index_oid, int attno,
			 IndexAMProperty prop, const char *propname,
			 bool *res, bool *isnull)
```
## Detailed Description
This function is a required part of the GiST access method interface that checks specific boolean properties of GiST indexes. While most access methods can rely on the core property code, GiST requires this custom implementation because it supports AMPROP_DISTANCE_ORDERABLE, which the core doesn't handle. The function also handles AMPROP_RETURNABLE to avoid the overhead of opening relations to call gistcanreturn.

The function examines the opclass (operator class) associated with the specified index column to determine whether specific support functions exist. For distance orderability, it checks for a distance function; for returnability, it checks for a fetch function or absence of a compress function.

## Parameters / Member Variables
- `index_oid`: OID of the index being queried
- `attno`: Column number within the index (must be > 0, as column-level inquiries only)
- `prop`: The IndexAMProperty being queried (AMPROP_DISTANCE_ORDERABLE or AMPROP_RETURNABLE)
- `*propname`: Name of the property (for error reporting, not used in current implementation)
- `*res`: Output parameter - set to the boolean result of the property check
- `*isnull`: Output parameter - set to true if the property value cannot be determined
## Dependencies
- Functions called/Symbols referenced:
  - [get_index_column_opclass](get_index_column_opclass.md)
  - [get_opclass_opfamily_and_input_type](get_opclass_opfamily_and_input_type.md)
  - SearchSysCacheExists4
  - [Int16GetDatum](../I/Int16GetDatum.md)
- Constants used:
  - AMPROP_DISTANCE_ORDERABLE
  - AMPROP_RETURNABLE
  - GIST_DISTANCE_PROC
  - GIST_FETCH_PROC
  - GIST_COMPRESS_PROC
- Called from:
  - [gisthandler](gisthandler.md) (as part of the GiST access method interface)

## Notes and Other Information
- Only handles column-level inquiries (attno > 0); returns false for index-level queries
- For AMPROP_DISTANCE_ORDERABLE: checks if the opclass provides a distance function with default types
- For AMPROP_RETURNABLE: returns true if either a fetch function exists OR no compress function exists (special case)
- The function assumes that if a distance function exists, there's a valid reason for it rather than searching through all operators
- Returns true on successful property evaluation (even if the property is false), false only if the property type is unsupported

## Simplified Source

```c
bool gistproperty(Oid index_oid, int attno,
                  IndexAMProperty prop, const char *propname,
                  bool *res, bool *isnull) {
    // Only handle column-level inquiries
    if (attno == 0)
        return false;

    // Determine which procedure to check based on property
    int16 procno;
    switch (prop) {
        case AMPROP_DISTANCE_ORDERABLE:
            procno = GIST_DISTANCE_PROC;
            break;
        case AMPROP_RETURNABLE:
            procno = GIST_FETCH_PROC;
            break;
        default:
            return false;
    }

    // Get column's opclass information
    Oid opclass = get_index_column_opclass(index_oid, attno);
    if (!OidIsValid(opclass)) {
        *isnull = true;
        return true;
    }

    // Get opfamily and input type from opclass
    Oid opfamily, opcintype;
    if (!get_opclass_opfamily_and_input_type(opclass, &opfamily, &opcintype)) {
        *isnull = true;
        return true;
    }

    // Check if the required function exists
    *res = SearchSysCacheExists4(AMPROCNUM,
                                 ObjectIdGetDatum(opfamily),
                                 ObjectIdGetDatum(opcintype),
                                 ObjectIdGetDatum(opcintype),
                                 Int16GetDatum(procno));

    // Special case: RETURNABLE is true if no compress function exists
    if (prop == AMPROP_RETURNABLE && !*res) {
        *res = !SearchSysCacheExists4(AMPROCNUM,
                                      ObjectIdGetDatum(opfamily),
                                      ObjectIdGetDatum(opcintype),
                                      ObjectIdGetDatum(opcintype),
                                      Int16GetDatum(GIST_COMPRESS_PROC));
    }

    *isnull = false;
    return true;
}
```