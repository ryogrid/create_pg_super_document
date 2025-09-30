# set_fn_opclass_options

## Location
[src/backend/utils/fmgr/fmgr.c:2070-2080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L2070-L2080)

## Overview
Sets opclass-specific options in an FmgrInfo structure by storing them as a bytea constant in the fn_expr field, enabling opclass support functions to access configuration data.

## Definition

```c
void
set_fn_opclass_options(FmgrInfo *flinfo, bytea *options)
```
## Detailed Description
This function provides a mechanism for opclass (operator class) support functions to receive configuration options. Since opclass support functions are invoked outside of normal expression contexts, the fn_expr field (which would normally contain the calling expression tree) can be repurposed to store opclass options as a constant node.

The function creates a Const node containing the options as bytea data, allowing support functions to retrieve their configuration through the standard FmgrInfo interface. This design leverages the existing infrastructure while providing a clean way to pass opclass-specific parameters to support functions.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure to be configured with opclass options
- : Bytea data containing opclass-specific configuration options (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [makeConst](../m/makeConst.md)
  - [PointerGetDatum](../P/PointerGetDatum.md) (macro)
- Constants referenced:
  - BYTEAOID
  - InvalidOid
- Called from (representative examples):
  - [index_getprocinfo](../i/index_getprocinfo.md)
  - [gincost_pattern](../g/gincost_pattern.md)

## Notes and Other Information
- The fn_expr field is repurposed since opclass support functions are called outside expression contexts
- Creates a BYTEAOID constant node with appropriate metadata (-1 for typmod, InvalidOid for collation)
- Handles NULL options gracefully by setting the isnull flag in the constant
- This mechanism allows opclass implementations to receive configuration without modifying the function call interface
- Primarily used in index access methods where opclass support functions need specific configuration parameters
- The use of fn_expr for this purpose is safe because opclass support functions are never called through normal expression evaluation paths

## Simplified Source

```c
void set_fn_opclass_options(FmgrInfo *flinfo, bytea *options) {
    // Store options as a bytea constant in fn_expr field
    flinfo->fn_expr = (Node *) makeConst(
        BYTEAOID,                    // type OID
        -1,                          // typmod
        InvalidOid,                  // collation
        -1,                          // typlen (variable length)
        PointerGetDatum(options),    // value
        options == NULL,             // isnull flag
        false                        // byval (pass by reference)
    );
}
```