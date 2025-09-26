# VarStringSortSupport

## Location
[src/backend/utils/adt/varlena.c:97-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L97-L107)

## Overview
VarStringSortSupport is a structure that provides sorting support state for variable-length string types, including memory management for string buffers, collation handling, and abbreviated key optimization for improved sort performance.

## Definition
```c
typedef struct
{
    char       *buf1;           /* 1st string, or abbreviation original string buf */
    char       *buf2;           /* 2nd string, or abbreviation strxfrm() buf */
    int         buflen1;        /* Allocated length of buf1 */
    int         buflen2;        /* Allocated length of buf2 */
    int         last_len1;      /* Length of last buf1 string/strxfrm() input */
    int         last_len2;      /* Length of last buf2 string/strxfrm() blob */
    int         last_returned;  /* Last comparison result (cache) */
    bool        cache_blob;     /* Does buf2 contain strxfrm() blob, etc? */
    bool        collate_c;
    Oid         typid;          /* Actual datatype (text/bpchar/bytea/name) */
    hyperLogLogState abbr_card; /* Abbreviated key cardinality state */
    hyperLogLogState full_card; /* Full key cardinality state */
    double      prop_card;      /* Required cardinality proportion */
    pg_locale_t locale;
} VarStringSortSupport;
```

## Detailed Description
VarStringSortSupport is a comprehensive state structure used by PostgreSQL's sorting infrastructure to optimize string comparisons and sorting operations. It manages reusable memory buffers to avoid repeated allocations, implements locale-aware string collation, and provides abbreviated key optimization for improved performance with large datasets.

The structure supports multiple string data types (text, bpchar, bytea, name) and can handle both C locale and locale-specific collations. It includes cardinality tracking using HyperLogLog for making intelligent decisions about when to use abbreviated keys versus full string comparisons.

## Parameters / Member Variables
- `buf1`: Primary buffer for storing the first string or original string for abbreviation
- `buf2`: Secondary buffer for storing the second string or strxfrm() transformation result
- `buflen1`: Allocated size of buf1 in bytes
- `buflen2`: Allocated size of buf2 in bytes
- `last_len1`: Length of the last string stored in buf1 or last strxfrm() input
- `last_len2`: Length of the last string stored in buf2 or last strxfrm() blob
- `last_returned`: Cached result of the most recent comparison operation
- `cache_blob`: Boolean indicating whether buf2 contains a strxfrm() transformation blob
- `collate_c`: Boolean indicating whether C locale collation is being used
- `typid`: PostgreSQL type OID identifying the actual datatype being sorted
- `abbr_card`: HyperLogLog state for tracking cardinality of abbreviated keys
- `full_card`: HyperLogLog state for tracking cardinality of full keys
- `prop_card`: Required cardinality proportion threshold for abbreviated key decisions
- `locale`: PostgreSQL locale object for locale-specific operations

## Dependencies
- Functions called/Symbols referenced:
  - hyperLogLogState (for cardinality tracking)
  - pg_locale_t (for locale handling)
  - Oid (for type identification)
- Called from (representative examples):
  - varstr_sortsupport
  - varstrfastcmp_locale
  - varstr_abbrev_convert
  - varstr_abbrev_abort

## Notes and Other Information
- This structure is central to PostgreSQL's string sorting optimization strategy
- The buffer reuse mechanism significantly reduces memory allocation overhead during sorting
- Abbreviated keys provide substantial performance improvements for large string sorts by using shorter comparison keys
- The HyperLogLog cardinality tracking helps determine when abbreviated keys are beneficial
- Supports multiple PostgreSQL string types: text, bpchar (char/varchar), bytea, and name
- The locale support enables proper international string sorting according to collation rules
- Memory buffers are managed to avoid frequent allocations during sort operations