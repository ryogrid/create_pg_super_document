# ResOwnerReleaseHMAC

## Location
[src/common/hmac_openssl.c:384-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/hmac_openssl.c#L384-L391)

## Overview
A callback function for PostgreSQL's resource owner system to automatically clean up HMAC contexts when resources are released.

## Definition
```c
static void ResOwnerReleaseHMAC(Datum res)
```

## Detailed Description
The ResOwnerReleaseHMAC function serves as a resource cleanup callback that is automatically invoked by PostgreSQL's resource management system when a resource owner is being destroyed or when resources need to be released. This function ensures that HMAC contexts are properly freed even if the calling code fails to explicitly clean them up, preventing memory leaks and ensuring sensitive cryptographic material is securely cleared. It converts the Datum parameter back to a HMAC context pointer, clears the resource owner reference, and calls pg_hmac_free to perform the actual cleanup.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the HMAC context to be released

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (to extract the context pointer)
  - [pg_hmac_free](../p/pg_hmac_free.md) (to actually free the context)
- Called from (representative examples):
  - Resource owner cleanup system (automatic invocation)
  - Referenced in HMAC context initialization for resource tracking

## Notes and Other Information
- Static function used internally within the HMAC implementation
- Part of PostgreSQL's resource management system for automatic cleanup
- Prevents memory leaks when transactions abort or connections are terminated
- Sets ctx->resowner to NULL before freeing to avoid double cleanup
- Essential for robust resource management in database server environment
- Only used when HMAC contexts are registered with resource owners
- Provides automatic cleanup even when explicit cleanup is forgotten

## Simplified Source

```c
static void ResOwnerReleaseHMAC(Datum res) {
    // Convert Datum back to HMAC context pointer
    pg_hmac_ctx *ctx = (pg_hmac_ctx *) DatumGetPointer(res);

    // Clear resource owner reference and free context
    ctx->resowner = NULL;
    pg_hmac_free(ctx);
}
```