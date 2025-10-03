# hashtextextended

## Location
[src/backend/access/hash/hashfunc.c:323-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L323-L382)

## Overview
An extended version of the hashtext function that supports seed-based hashing for text data types with collation awareness.

## Definition

```c
struct varlena *key = PG_GETARG_VARLENA_PP(0);
```
## Detailed Description
The hashtextextended function is the extended variant of hashtext that accepts an additional seed parameter for hash computation. Like hashtext, it handles collation-aware hashing by determining the appropriate locale and choosing between direct hashing for deterministic locales and transformation-based hashing for non-deterministic locales. The seed parameter allows for creating different hash values from the same input, which is useful for hash table resizing and hash-based algorithms that require multiple hash functions.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The text value to be hashed (text*)
  -  (arg 1): 64-bit seed value for hash computation (int64)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - [pg_locale_t](../p/pg_locale_t.md)
  - [lc_collate_is_c](../l/lc_collate_is_c.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md)
  - [hash_any_extended](hash_any_extended.md)
  - PG_GETARG_INT64
  - [pg_strnxfrm](../p/pg_strnxfrm.md)

- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Provides seed-based hashing capability for advanced hash table operations
- Maintains the same collation-aware behavior as hashtext
- Uses hash_any_extended instead of hash_any to incorporate the seed parameter
- Follows identical error handling and memory management patterns as hashtext
- Located at src/backend/access/hash/hashfunc.c:323-382

## Simplified Source

```c
Datum hashtextextended(PG_FUNCTION_ARGS) {
    text *key = PG_GETARG_TEXT_PP(0);
    Oid collid = PG_GET_COLLATION();
    pg_locale_t mylocale = 0;
    Datum result;

    // Check that collation is specified
    if (!collid)
        ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_COLLATION),
                       errmsg("could not determine which collation to use for string hashing")));

    // Get locale for non-C collations
    if (!lc_collate_is_c(collid))
        mylocale = pg_newlocale_from_collation(collid);

    if (pg_locale_deterministic(mylocale)) {
        // For deterministic locales, hash the text directly
        result = hash_any_extended((unsigned char *) VARDATA_ANY(key),
                                  VARSIZE_ANY_EXHDR(key),
                                  PG_GETARG_INT64(1));
    } else {
        // For non-deterministic locales, transform text first
        const char *keydata = VARDATA_ANY(key);
        size_t keylen = VARSIZE_ANY_EXHDR(key);

        Size bsize = pg_strnxfrm(NULL, 0, keydata, keylen, mylocale);
        char *buf = palloc(bsize + 1);

        pg_strnxfrm(buf, bsize + 1, keydata, keylen, mylocale);

        // Hash the transformed string (including NUL terminator)
        result = hash_any_extended((uint8_t *) buf, bsize + 1,
                                  PG_GETARG_INT64(1));
        pfree(buf);
    }

    PG_FREE_IF_COPY(key, 0);
    return result;
}
```