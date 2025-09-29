# printtup_prepare_info

## Location
[src/backend/access/common/printtup.c:250-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L250-L303)

## Overview
The printtup_prepare_info function prepares per-attribute information needed for efficient tuple output by setting up format-specific output functions and metadata for each column.

## Definition
```c
static void printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
```

## Detailed Description
This function initializes the PrinttupAttrInfo array that contains cached information for efficiently converting and outputting each attribute in a tuple. It determines the output format (text or binary) for each column and sets up the appropriate output functions using the function manager. For text format (format 0), it uses getTypeOutputInfo to get the text output function, while for binary format (format 1), it uses getTypeBinaryOutputInfo to get the binary output function. The function handles memory management by freeing any existing attribute info before allocating new structures. Error handling ensures that only supported format codes are accepted.

## Parameters / Member Variables
- `myState`: DR_printtup structure containing the receiver state and configuration
- `typeinfo`: TupleDesc describing the structure and types of the tuple attributes
- `numAttrs`: Number of attributes to prepare information for

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](pfree.md) (memory deallocation)
  - [palloc0](palloc0.md) (zero-initialized memory allocation)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (text output function lookup)
  - [getTypeBinaryOutputInfo](../g/getTypeBinaryOutputInfo.md) (binary output function lookup)
  - [fmgr_info](../f/fmgr_info.md) (function manager info setup)
  - ereport, errcode, errmsg (error reporting)
  - TupleDescAttr (attribute access macro)
  - [PrinttupAttrInfo](../P/PrinttupAttrInfo.md) (per-attribute information structure)
- Called from (representative examples):
  - [printtup](printtup.md)

## Notes and Other Information
- Supports two format codes: 0 (text format) and 1 (binary format)
- Caches function manager information for performance during tuple output
- Handles memory cleanup by freeing existing myinfo before reallocating
- Early return optimization when numAttrs is 0 or negative
- Uses portal formats array to determine the output format for each column
- Format-specific setup ensures optimal performance by avoiding format checks during actual tuple output
- Error reporting for unsupported format codes helps catch protocol violations

## Simplified Source

```c
static void
printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
{
    int16 *formats = myState->portal->formats;
    int i;

    // Clean up any existing attribute info
    if (myState->myinfo)
        pfree(myState->myinfo);
    myState->myinfo = NULL;

    myState->attrinfo = typeinfo;
    myState->nattrs = numAttrs;

    if (numAttrs <= 0)
        return;

    // Allocate array for per-attribute information
    myState->myinfo = (PrinttupAttrInfo *)
        palloc0(numAttrs * sizeof(PrinttupAttrInfo));

    // Set up output functions for each attribute
    for (i = 0; i < numAttrs; i++)
    {
        PrinttupAttrInfo *thisState = myState->myinfo + i;
        int16 format = (formats ? formats[i] : 0);
        Form_pg_attribute attr = TupleDescAttr(typeinfo, i);

        thisState->format = format;

        if (format == 0)  // Text format
        {
            getTypeOutputInfo(attr->atttypid,
                            &thisState->typoutput,
                            &thisState->typisvarlena);
            fmgr_info(thisState->typoutput, &thisState->finfo);
        }
        else if (format == 1)  // Binary format
        {
            getTypeBinaryOutputInfo(attr->atttypid,
                                  &thisState->typsend,
                                  &thisState->typisvarlena);
            fmgr_info(thisState->typsend, &thisState->finfo);
        }
        else
        {
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unsupported format code: %d", format)));
        }
    }
}
```