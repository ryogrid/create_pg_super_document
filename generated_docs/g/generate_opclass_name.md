# generate_opclass_name

## Location
[src/backend/utils/adt/ruleutils.c:12569-12590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12569-L12590)

## Overview
Computes the display name for an operator class specified by OID, including all necessary quoting and schema-prefixing.

## Definition
```c
char *generate_opclass_name(Oid opclass)
```

## Detailed Description
This function is a wrapper around get_opclass_name that provides a convenient interface for obtaining a fully qualified operator class name as a standalone string. It creates a temporary StringInfo buffer, calls get_opclass_name with InvalidOid as the actual_datatype parameter (ensuring the name is always generated regardless of default status), and returns the resulting string with the leading space removed.

The function ensures that the returned name includes proper identifier quoting and schema qualification when necessary, making it suitable for use in contexts where a complete operator class specification is needed.

## Parameters / Member Variables
- `opclass`: The OID of the operator class to generate a name for

## Dependencies
- Functions called/Symbols referenced:
  - [get_opclass_name](get_opclass_name.md) (performs the actual name generation logic)
  - [initStringInfo](../i/initStringInfo.md) (initializes string buffer)
- Called from (representative examples):
  - [index_opclass_options](../i/index_opclass_options.md) (in indexam.c)

## Notes and Other Information
- Always generates the operator class name regardless of whether it's the default (passes InvalidOid as actual_datatype)
- Returns a pointer to the buffer data starting from position 1 to skip the leading space that get_opclass_name prepends
- The returned string includes proper schema qualification and identifier quoting
- Primarily used when a standalone operator class name string is needed rather than appending to an existing buffer

## Simplified Source

```c
char *generate_opclass_name(Oid opclass) {
    StringInfoData buf;

    // Initialize string buffer and get opclass name
    initStringInfo(&buf);
    get_opclass_name(opclass, InvalidOid, &buf);

    // Return buffer data, skipping leading space added by get_opclass_name
    return &buf.data[1];
}
```