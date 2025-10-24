# inet_gist_fetch

## Location
[src/backend/utils/adt/network_gist.c:590-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L590-L619)

## Overview
The GiST fetch function for the inet data type that reconstructs the original inet datum from the internal GistInetKey representation.

## Definition
```c
Datum inet_gist_fetch(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the fetch method in the GiST operator class for inet values. It performs the inverse operation of inet_gist_compress by converting a GistInetKey back into a standard inet datum. This is essential when the index needs to return actual inet values to the query executor.

The function reconstructs the inet structure by:
1. Extracting the compressed data from the GistInetKey (IP family, network bits, and address bytes)
2. Allocating memory for a new inet structure
3. Copying the IP family, network mask bits, and raw address data
4. Setting the appropriate variable-length header
5. Creating a new GISTENTRY containing the reconstructed inet value

This operation is crucial for index-only scans and other scenarios where the original inet value must be retrieved from index data without accessing the heap.

## Parameters / Member Variables
- `entry`: GISTENTRY pointer containing the GistInetKey to be converted back to inet format

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInetKeyP: Extracts GistInetKey pointer from Datum
  - gk_ip_family: Gets IP family from GistInetKey
  - gk_ip_minbits: Gets minimum bits from GistInetKey
  - gk_ip_addr: Gets address buffer from GistInetKey
  - ip_family: Sets IP family in inet structure
  - ip_bits: Sets network bits in inet structure
  - ip_addr: Gets address buffer in inet structure
  - ip_addrsize: Gets address size for inet structure
  - SET_INET_VARSIZE: Sets variable size header for inet
  - [InetPGetDatum](../I/InetPGetDatum.md): Converts inet pointer to Datum
  - gistentryinit: Initializes GISTENTRY structure
  - [palloc](../p/palloc.md)/palloc0: PostgreSQL memory allocation functions

- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in operator class)

## Notes and Other Information
- This is the counterpart to inet_gist_compress, performing the reverse transformation
- Unlike compress/decompress pairs in some other data types, this function fully reconstructs the original inet value
- The function is essential for index-only scans where query results are obtained entirely from index data
- Memory is allocated using palloc, which is automatically freed at transaction end
- The reconstructed inet maintains all the original information including IP family and network mask bits
- File location: src/backend/utils/adt/network_gist.c:590-619

## Simplified Source

```c
Datum
inet_gist_fetch(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    GistInetKey *key = DatumGetInetKeyP(entry->key);
    GISTENTRY *retval;
    inet *dst;

    // Allocate and initialize new inet structure
    dst = (inet *) palloc0(sizeof(inet));

    // Copy data from GistInetKey to inet
    ip_family(dst) = gk_ip_family(key);
    ip_bits(dst) = gk_ip_minbits(key);
    memcpy(ip_addr(dst), gk_ip_addr(key), ip_addrsize(dst));
    SET_INET_VARSIZE(dst);

    // Create return GISTENTRY with reconstructed inet
    retval = palloc(sizeof(GISTENTRY));
    gistentryinit(*retval, InetPGetDatum(dst), entry->rel, entry->page,
                  entry->offset, false);

    PG_RETURN_POINTER(retval);
}
```