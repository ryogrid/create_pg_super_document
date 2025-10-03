# ri_GenerateQualCollation

## Location
[src/backend/utils/adt/ri_triggers.c:1939-1979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1939-L1979)

## Overview
A utility function that adds a COLLATE specification to a WHERE clause in dynamically constructed SQL queries, ensuring proper collation handling for referential integrity operations.

## Definition

```c
static void
ri_GenerateQualCollation(StringInfo buf, Oid collation)
```
## Detailed Description
This function appends a COLLATE clause to a StringInfo buffer to specify the collation to be used for string comparisons in referential integrity queries. The function is crucial for handling cases where referencing and referenced columns have different collations, as required by the SQL standard which specifies that RI comparisons should use the referenced column's collation.

The function performs the following operations:
1. Checks if the collation is valid (non-collatable data types are ignored)
2. Looks up the collation information in the system catalog
3. Retrieves both the collation name and its namespace
4. Constructs a fully qualified COLLATE clause in the format "COLLATE schema.collation"
5. Uses proper quoting to ensure the query is not search-path-dependent

The function intentionally avoids being used for queries comparing variables to parameters, as this allows for better index usage on referencing columns while maintaining semantic correctness due to all collations having the same notion of equality.

## Parameters / Member Variables
- : StringInfo buffer to which the COLLATE clause will be appended
- : OID of the collation to be applied; if invalid (OidIsValid returns false), the function returns without action

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if the collation OID is valid
  - : Looks up collation information in the system cache
  - : Validates the returned tuple from cache lookup
  - : Extracts the collation form structure from the heap tuple
  - : Extracts string from a Name structure
  - : Retrieves the namespace name for the collation
  - : Safely quotes individual names for SQL usage
  - : Appends formatted strings to the buffer
  - : Releases the system cache reference

- Called from (representative examples):
  - : Used in foreign key restriction operations
  - : Used in foreign key cascade delete operations
  - : Used in foreign key cascade update operations
  - : Used in referential integrity set operations
  - : Used in initial referential integrity constraint checks
  - : Used in partition removal integrity checks

## Notes and Other Information
- This is a static function within ri_triggers.c, specifically designed for referential integrity operations
- The function ensures queries are not search-path-dependent by always qualifying collation names with their schema
- For non-collatable data types, the function returns immediately without adding any COLLATE clause
- The SQL standard requires RI comparisons to use the referenced column's collation, but PostgreSQL optimizes by using the referencing column's collation when possible for better index usage
- Essential for resolving collation conflicts when directly comparing columns with different collations in referential integrity constraints
- Uses the system catalog cache for efficient collation lookup and includes proper error handling for missing collations

## Simplified Source

```c
static void ri_GenerateQualCollation(StringInfo buf, Oid collation) {
    HeapTuple tp;
    Form_pg_collation colltup;
    char *collname;
    char onename[MAX_QUOTED_NAME_LEN];

    // Skip if data type is not collatable
    if (!OidIsValid(collation))
        return;

    // Look up collation information in system catalog
    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for collation %u", collation);

    colltup = (Form_pg_collation) GETSTRUCT(tp);
    collname = NameStr(colltup->collname);

    // Generate fully qualified COLLATE clause: schema.collation
    quoteOneName(onename, get_namespace_name(colltup->collnamespace));
    appendStringInfo(buf, " COLLATE %s", onename);
    quoteOneName(onename, collname);
    appendStringInfo(buf, ".%s", onename);

    ReleaseSysCache(tp);
}
```