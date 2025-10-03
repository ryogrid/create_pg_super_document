# TupleDescInitEntry

## Location
[src/backend/access/common/tupdesc.c:651-725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L651-L725)

## Overview
TupleDescInitEntry initializes a single attribute structure within a previously allocated TupleDesc, setting up all necessary field values by looking up type information from the system catalog.

## Definition

```c
void
TupleDescInitEntry(TupleDesc desc,
				   AttrNumber attributeNumber,
				   const char *attributeName,
				   Oid oidtypeid,
				   int32 typmod,
				   int attdim)
```
## Detailed Description
This function performs comprehensive initialization of a single attribute slot in a TupleDesc structure. It combines caller-provided information with type metadata retrieved from the system catalog to fully populate the pg_attribute structure.

The initialization process includes:
1. **Basic attribute setup**: Sets attribute number, name (if provided), type modifier, and array dimensions
2. **Type information retrieval**: Performs a system catalog lookup to get detailed type information including length, alignment, storage mode, and collation
3. **Default value initialization**: Sets reasonable defaults for constraint-related fields like nullability, defaults, and inheritance properties
4. **Storage attribute setup**: Configures physical storage characteristics based on the type definition

The function handles several special cases:
- NULL attribute names are replaced with empty strings
- Existing attribute names can be preserved by passing the current name
- Array dimensions are validated against system limits
- Default collation is set based on the type's collation property

## Parameters / Member Variables
- `desc`: The TupleDesc structure containing the attribute to initialize
- `attributeNumber`: 1-based attribute position within the TupleDesc (must be valid)
- `*attributeName`: Name for the attribute (NULL creates empty name, existing name preserves current name)
- `oidtypeid`: PostgreSQL type OID identifying the data type
- `typmod`: Type modifier value for the attribute
- `attdim`: Number of array dimensions (0 for non-arrays, validated against PG_INT16_MAX)
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (type catalog structure access)
  - PointerIsValid (validation macro)
  - PG_INT16_MAX (array dimension limit)
  - MemSet (memory clearing)
  - NAMEDATALEN (name length constant)
  - [namestrcpy](../n/namestrcpy.md) (name copying utility)
  - InvalidCompressionMethod (compression default)
- Called from (representative examples):
  - [BuildDescFromLists](../B/BuildDescFromLists.md) (tuple descriptor construction)
  - [ExecTypeFromTLInternal](../E/ExecTypeFromTLInternal.md) (executor type setup)
  - [create_toast_table](../c/create_toast_table.md) (TOAST table creation)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md) (parser function handling)

## Notes and Other Information
- Performs system catalog lookup via SearchSysCache1/ReleaseSysCache for type metadata
- Sets attcollation to the default collation for the specified data type
- For non-default collations, use TupleDescInitEntryCollation afterwards
- Initializes physical storage fields (attlen, attbyval, attalign, attstorage) from type definition
- Sets conservative defaults for constraint fields (attnotnull=false, atthasdef=false, etc.)
- Widely used throughout PostgreSQL for dynamic tuple descriptor construction
- Critical component in function result type determination and table creation processes

## Simplified Source

```c
void TupleDescInitEntry(TupleDesc desc,
                        AttrNumber attributeNumber,
                        const char *attributeName,
                        Oid oidtypeid,
                        int32 typmod,
                        int attdim)
{
    HeapTuple tuple;
    Form_pg_type typeForm;
    Form_pg_attribute att;

    // Sanity checks
    Assert(PointerIsValid(desc));
    Assert(attributeNumber >= 1);
    Assert(attributeNumber <= desc->natts);
    Assert(attdim >= 0);
    Assert(attdim <= PG_INT16_MAX);

    // Get attribute structure to initialize
    att = TupleDescAttr(desc, attributeNumber - 1);

    // Set basic attribute properties
    att->attrelid = 0;  // dummy value
    att->attcacheoff = -1;
    att->atttypmod = typmod;
    att->attnum = attributeNumber;
    att->attndims = attdim;

    // Handle attribute name
    if (attributeName == NULL)
        MemSet(NameStr(att->attname), 0, NAMEDATALEN);
    else if (attributeName != NameStr(att->attname))
        namestrcpy(&(att->attname), attributeName);

    // Set default constraint values
    att->attnotnull = false;
    att->atthasdef = false;
    att->atthasmissing = false;
    att->attidentity = '\0';
    att->attgenerated = '\0';
    att->attisdropped = false;
    att->attislocal = true;
    att->attinhcount = 0;

    // Look up type information in system catalog
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oidtypeid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for type %u", oidtypeid);
    typeForm = (Form_pg_type) GETSTRUCT(tuple);

    // Copy type properties from catalog
    att->atttypid = oidtypeid;
    att->attlen = typeForm->typlen;
    att->attbyval = typeForm->typbyval;
    att->attalign = typeForm->typalign;
    att->attstorage = typeForm->typstorage;
    att->attcompression = InvalidCompressionMethod;
    att->attcollation = typeForm->typcollation;

    ReleaseSysCache(tuple);
}
```