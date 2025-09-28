# nodeToStringInternal

## Location
[src/backend/nodes/outfuncs.c:770-790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L770-L790)

## Overview
A static helper function that converts a PostgreSQL node structure to its ASCII string representation, with control over whether location fields are included in the output.

## Definition
static char *nodeToStringInternal(const void *obj, bool write_loc_fields)

## Detailed Description
nodeToStringInternal is the core implementation function for converting PostgreSQL parse tree nodes and other structures into their string representations. It serves as the common backend for both nodeToString and nodeToStringWithLocations functions. The function creates a StringInfo buffer, temporarily sets a global flag to control location field output, calls the main output function (outNode), and then restores the original location field setting. This approach allows for consistent string conversion while providing flexibility in whether debugging location information is included.

## Parameters / Member Variables
- obj: A pointer to the PostgreSQL node or structure to be converted to string format
- write_loc_fields: A boolean flag that determines whether location fields should be output with their actual values (true) or as -1 (false)

## Dependencies
- Functions called/Symbols referenced:
  - [outNode](../o/outNode.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [StringInfoData](../S/StringInfoData.md) (struct)
- Called from (representative examples):
  - [nodeToString](nodeToString.md)
  - [nodeToStringWithLocations](nodeToStringWithLocations.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the outfuncs.c file
- The function manipulates the global write_location_fields variable to control output format
- Location fields are typically set to -1 in most use cases since the original query string is usually not available
- The actual location values can be useful for debugging purposes
- Memory for the returned string is allocated using PostgreSQL's palloc mechanism
- The function follows PostgreSQL's pattern of using StringInfo for efficient string building

## Simplified Source

```c
// Simplified version of nodeToStringInternal
static char *nodeToStringInternal(const void *obj, bool write_loc_fields) {
    StringInfoData str;
    bool save_write_location_fields;

    // Save current location field setting
    save_write_location_fields = write_location_fields;
    write_location_fields = write_loc_fields;

    // Initialize string buffer and convert node to string
    initStringInfo(&str);
    outNode(&str, obj);

    // Restore original location field setting
    write_location_fields = save_write_location_fields;

    return str.data;
}
```

Key simplifications made:
- Preserved essential location field control mechanism
- Maintained string buffer initialization and node output
- Kept global state save/restore pattern
- Focused on core node-to-string conversion functionality