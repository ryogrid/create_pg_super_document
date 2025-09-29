# serialize_prepare_info

## Location
[src/backend/commands/explain.c:5334-5386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5334-L5386)

## Overview
A helper function that prepares function lookup information needed for tuple serialization in different output formats (text or binary).

## Definition
static void serialize_prepare_info(SerializeDestReceiver *receiver, TupleDesc typeinfo, int nattrs)

## Detailed Description
This function sets up the necessary function lookup information for serializing tuples to different output formats. It's a simplified version of printtup_prepare_info() that handles format preparation for tuple serialization. The function allocates an array of FmgrInfo structures and populates each with the appropriate output function based on the format specification. For text format (format 0), it uses type output functions via getTypeOutputInfo, while for binary format (format 1), it uses binary output functions via getTypeBinaryOutputInfo. The function also handles cleanup of any previously allocated function info and validates that the format code is supported.

## Parameters / Member Variables
- receiver: SerializeDestReceiver pointer containing the destination receiver state and format information
- typeinfo: TupleDesc describing the tuple structure and attribute types
- nattrs: Number of attributes in the tuple descriptor

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - TupleDescAttr (tuple descriptor access macro)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (type system function for text output)
  - [getTypeBinaryOutputInfo](../g/getTypeBinaryOutputInfo.md) (type system function for binary output)
  - [fmgr_info](../f/fmgr_info.md) (function manager info initialization)
  - ereport (error reporting)
- Called from (representative examples):
  - [serializeAnalyzeReceive](serializeAnalyzeReceive.md)

## Notes and Other Information
- This is a static function only accessible within the explain.c file
- Supports both text (format 0) and binary (format 1) wire protocol formats
- Handles memory management by freeing old function info before allocating new
- Part of PostgreSQL's tuple serialization system for EXPLAIN command output
- Includes error handling for unsupported format codes

## Simplified Source

```c
static void serialize_prepare_info(SerializeDestReceiver *receiver,
                                  TupleDesc typeinfo, int nattrs)
{
    // Clean up any old function info
    if (receiver->finfos)
        pfree(receiver->finfos);
    receiver->finfos = NULL;

    // Store tuple descriptor info
    receiver->attrinfo = typeinfo;
    receiver->nattrs = nattrs;

    // Early exit if no attributes
    if (nattrs <= 0)
        return;

    // Allocate function info array
    receiver->finfos = (FmgrInfo *) palloc0(nattrs * sizeof(FmgrInfo));

    // Set up function info for each attribute
    for (int i = 0; i < nattrs; i++)
    {
        FmgrInfo *finfo = receiver->finfos + i;
        Form_pg_attribute attr = TupleDescAttr(typeinfo, i);
        Oid typoutput;
        Oid typsend;
        bool typisvarlena;

        if (receiver->format == 0)
        {
            // Text format: get type output function
            getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
            fmgr_info(typoutput, finfo);
        }
        else if (receiver->format == 1)
        {
            // Binary format: get type send function
            getTypeBinaryOutputInfo(attr->atttypid, &typsend, &typisvarlena);
            fmgr_info(typsend, finfo);
        }
        else
        {
            // Unsupported format
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unsupported format code: %d", receiver->format)));
        }
    }
}
```