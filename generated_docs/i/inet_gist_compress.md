# inet_gist_compress

## Location
[src/backend/utils/adt/network_gist.c:542-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L542-L589)

## Overview
The GiST compress function for the inet data type that converts inet values to the internal GistInetKey representation used in GiST indexing.

## Definition
```c
Datum inet_gist_compress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's GiST (Generalized Search Tree) indexing infrastructure for the inet data type. It serves as the compress method in the GiST operator class, responsible for converting leaf-level inet values into the internal GistInetKey format that is stored in the index.

The function handles two main cases:
1. **Leaf keys**: When processing actual inet values from table rows, it creates a new GistInetKey structure containing the IP family, network bits, common bits, and address data
2. **Internal nodes**: For non-leaf entries, it returns the entry unchanged as it already contains compressed data

The compression process extracts essential information from the inet value including the IP family (IPv4/IPv6), network mask bits, and raw address bytes, storing them in a compact GistInetKey structure optimized for index operations.

## Parameters / Member Variables
- `entry`: GISTENTRY pointer containing the inet value to be compressed along with metadata (relation, page, offset)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md): Extracts inet pointer from Datum
  - ip_family: Gets IP family (IPv4/IPv6) from inet
  - ip_bits: Gets network mask bits from inet  
  - ip_addr: Gets raw address bytes from inet
  - gk_ip_family: Sets IP family in GistInetKey
  - gk_ip_minbits: Sets minimum bits in GistInetKey
  - gk_ip_maxbits: Gets maximum bits for GistInetKey
  - gk_ip_commonbits: Sets common bits in GistInetKey
  - gk_ip_addr: Gets address buffer in GistInetKey
  - gk_ip_addrsize: Gets address size in GistInetKey
  - SET_GK_VARSIZE: Sets variable size header for GistInetKey
  - gistentryinit: Initializes GISTENTRY structure
  - [palloc](../p/palloc.md)/palloc0: PostgreSQL memory allocation functions

- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in operator class)

## Notes and Other Information
- This function is registered as part of the inet GiST operator class and called automatically during index operations
- The GistInetKey format allows efficient comparison and union operations needed for GiST tree maintenance
- Memory allocation uses PostgreSQL's palloc which is automatically freed at transaction end
- The function handles NULL values by initializing entries with zero Datum
- File location: src/backend/utils/adt/network_gist.c:542-589

## Simplified Source

```c
Datum
inet_gist_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    GISTENTRY *retval;

    if (entry->leafkey)
    {
        // Compress inet value to GistInetKey format for leaf entries
        retval = palloc(sizeof(GISTENTRY));

        if (DatumGetPointer(entry->key) != NULL)
        {
            inet *in = DatumGetInetPP(entry->key);
            GistInetKey *r = palloc0(sizeof(GistInetKey));

            // Extract and store inet components in compressed format
            gk_ip_family(r) = ip_family(in);
            gk_ip_minbits(r) = ip_bits(in);
            gk_ip_commonbits(r) = gk_ip_maxbits(r);
            memcpy(gk_ip_addr(r), ip_addr(in), gk_ip_addrsize(r));
            SET_GK_VARSIZE(r);

            gistentryinit(*retval, PointerGetDatum(r),
                          entry->rel, entry->page, entry->offset, false);
        }
        else
        {
            // Handle NULL input
            gistentryinit(*retval, (Datum) 0,
                          entry->rel, entry->page, entry->offset, false);
        }
    }
    else
    {
        // Internal nodes already compressed - return as-is
        retval = entry;
    }

    PG_RETURN_POINTER(retval);
}
```