# fmgr_info_copy

## Location
[src/backend/utils/fmgr/fmgr.c:580-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L580-L594)

## Overview
Creates a copy of an FmgrInfo structure while handling memory context management and resetting language-dependent subsidiary information.

## Definition

```c
struct fmgr_security_definer_cache
{
	FmgrInfo	flinfo;			/* lookup info for target function */
	Oid			userid;			/* userid to set, or InvalidOid */
	List	   *configNames;	/* GUC names to set, or NIL */
	List	   *configHandles;	/* GUC handles to set, or NIL */
	List	   *configValues;	/* GUC values to set, or NIL */
	Datum		arg;			/* passthrough argument for plugin modules */
};
```
## Detailed Description
The  function performs a shallow copy of an FmgrInfo structure from source to destination. This function is necessary when FmgrInfo structures need to be duplicated across different memory contexts, such as when copying function call information from plan-time to execution-time contexts.

The function handles the inherent complexity of copying function manager information by taking a conservative approach: it copies all the basic function metadata but deliberately zeros out the  field. This is because  may contain language-specific subsidiary information that cannot be reliably duplicated across memory contexts. By resetting this field, the function ensures that any language-dependent state will be recomputed when needed in the new context.

## Parameters
- `dstinfo`: FmgrInfo pointer to the destination structure where the copy will be stored
- `srcinfo`: FmgrInfo pointer to the source structure to be copied
- `destcxt`: MemoryContext where the destination FmgrInfo will reside

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
- Called from (representative examples):
  - [initGinState](../i/initGinState.md) (GIN index operations)
  - [initGISTstate](../i/initGISTstate.md) (GiST index operations)
  - [ScanKeyEntryInitializeWithInfo](../S/ScanKeyEntryInitializeWithInfo.md)
  - fmgr_info_set_expr
  - Various BRIN index functions

## Notes and Other Information
- The function explicitly sets  to NULL, requiring subsidiary info to be recomputed
- This is a 'bogus' operation by design due to the complexity of duplicating language-dependent state
- The destination memory context is explicitly set to ensure proper memory management
- Widely used throughout PostgreSQL's index access methods and query execution
- Essential for copying function information between different execution phases

## Simplified Source

```c
void
fmgr_info_copy(FmgrInfo *dstinfo, FmgrInfo *srcinfo,
               MemoryContext destcxt)
{
    // Copy the entire FmgrInfo structure
    memcpy(dstinfo, srcinfo, sizeof(FmgrInfo));

    // Set the destination memory context
    dstinfo->fn_mcxt = destcxt;

    // Clear language-dependent subsidiary info (will be recomputed if needed)
    dstinfo->fn_extra = NULL;
}
```