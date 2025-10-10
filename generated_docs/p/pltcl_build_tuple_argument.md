# pltcl_build_tuple_argument

## Location
[src/pl/tcl/pltcl.c:3104-3179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L3104-L3179)

## Overview
Builds a Tcl list object suitable for 'array set' from all attributes of a given tuple, converting PostgreSQL tuple data into a format that can be consumed by Tcl functions.

## Definition

```c
static Tcl_Obj *
pltcl_build_tuple_argument(HeapTuple tuple, TupleDesc tupdesc, bool include_generated)
```
## Detailed Description
This function converts a PostgreSQL HeapTuple into a Tcl list object that can be used with Tcl's 'array set' command. It iterates through all attributes in the tuple descriptor, extracts each attribute's name and value from the tuple, and creates a Tcl list containing alternating attribute names and their string representations. The function handles type conversion by using PostgreSQL's output functions to convert each attribute value to its string representation, then converts the strings from PostgreSQL's encoding to UTF-8 for Tcl compatibility.

The function skips dropped attributes and can optionally include or exclude generated columns based on the include_generated parameter. For non-null values, it retrieves the appropriate output function for the attribute's data type and converts the value to a string format that Tcl can understand.

## Parameters / Member Variables
- `tuple`: The HeapTuple containing the row data to be converted
- `tupdesc`: The TupleDesc describing the structure and metadata of the tuple
- `include_generated`: Boolean flag indicating whether to include generated columns in the result
## Dependencies
- Functions called/Symbols referenced:
  - [heap_getattr](../h/heap_getattr.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)  
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - UTF_BEGIN
  - UTF_E2U
  - UTF_END
  - Tcl_NewObj
  - Tcl_ListObjAppendElement
  - Tcl_NewStringObj
  - TupleDescAttr
  - NameStr
  - [pfree](pfree.md)
- Called from (representative examples):
  - [pltcl_func_handler](pltcl_func_handler.md)
  - [pltcl_trigger_handler](pltcl_trigger_handler.md)

## Notes and Other Information
- The function creates a flat list suitable for Tcl's 'array set' command, where each pair of consecutive elements represents an attribute name and its value
- The function handles UTF-8 encoding conversion using UTF_BEGIN/UTF_E2U/UTF_END macros to ensure proper character encoding between PostgreSQL and Tcl
- Null attributes are skipped entirely rather than being represented in the list, which may require careful handling by calling functions
- Generated columns are only included when explicitly requested via the include_generated parameter
- Memory management is handled by freeing the outputstr after use with pfree()

## Simplified Source

```c
static Tcl_Obj *
pltcl_build_tuple_argument(HeapTuple tuple, TupleDesc tupdesc, bool include_generated)
{
    Tcl_Obj *retobj = Tcl_NewObj();
    int i;
    char *outputstr;
    Datum attr;
    bool isnull;
    char *attname;
    Oid typoutput;
    bool typisvarlena;

    // Iterate through all attributes in the tuple
    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        // Skip dropped attributes
        if (att->attisdropped)
            continue;

        // Skip generated columns unless requested
        if (att->attgenerated && !include_generated)
            continue;

        // Get attribute name
        attname = NameStr(att->attname);

        // Get attribute value
        attr = heap_getattr(tuple, i + 1, tupdesc, &isnull);

        // Only process non-null values
        if (!isnull)
        {
            // Convert value to string using appropriate output function
            getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena);
            outputstr = OidOutputFunctionCall(typoutput, attr);

            // Add attribute name and value to Tcl list (for 'array set')
            UTF_BEGIN;
            Tcl_ListObjAppendElement(NULL, retobj,
                                     Tcl_NewStringObj(UTF_E2U(attname), -1));
            UTF_END;
            UTF_BEGIN;
            Tcl_ListObjAppendElement(NULL, retobj,
                                     Tcl_NewStringObj(UTF_E2U(outputstr), -1));
            UTF_END;
            pfree(outputstr);
        }
    }

    return retobj;
}
```