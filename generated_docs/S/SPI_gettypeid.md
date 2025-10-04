# SPI_gettypeid

## Location
[src/backend/executor/spi.c:1308-1325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1308-L1325)

## Overview
Retrieves the PostgreSQL type OID (Object Identifier) for a specified column from a tuple descriptor.

## Definition
```c
Oid SPI_gettypeid(TupleDesc tupdesc, int fnumber)
```

## Detailed Description
SPI_gettypeid extracts the type OID for a specified column from a tuple descriptor. This function provides direct access to the internal type identifier used by PostgreSQL's type system. It handles both regular user attributes (positive fnumber) and system attributes (negative fnumber), performing validation on the attribute number before returning the appropriate type OID.

The function is designed for performance-critical scenarios where only the type OID is needed, avoiding the overhead of type name lookup that SPI_gettype performs. For additional type metadata like typmod and typcollation, the comment suggests direct inspection of the TupleDesc structure.

## Parameters / Member Variables
- `tupdesc`: Tuple descriptor containing column information  
- `fnumber`: 1-based column number for regular attributes, or negative number for system attributes

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [SystemAttributeDefinition](SystemAttributeDefinition.md)  
  - FirstLowInvalidHeapAttributeNumber
  - SPI_ERROR_NOATTRIBUTE
  - InvalidOid
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (src/backend/utils/adt/tsquery_rewrite.c)
  - [ts_stat_sql](../t/ts_stat_sql.md) (src/backend/utils/adt/tsvector_op.c)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md) (src/backend/utils/adt/tsvector_op.c)
  - [SPI_sql_row_to_xmlelement](SPI_sql_row_to_xmlelement.md) (src/backend/utils/adt/xml.c)
  - [ttdummy](../t/ttdummy.md) (src/test/regress/regress.c)

## Notes and Other Information
- Returns InvalidOid and sets SPI_result to SPI_ERROR_NOATTRIBUTE if fnumber is invalid
- More efficient than SPI_gettype when only the type OID is needed
- Supports both regular attributes (fnumber > 0) and system attributes (fnumber < 0)
- Sets global SPI_result to 0 on success
- Does not provide access to typmod or typcollation - direct TupleDesc inspection is recommended for those
- Commonly used in type checking and validation scenarios in SPI-based code

## Simplified Source

```c
Oid SPI_gettypeid(TupleDesc tupdesc, int fnumber) {
    SPI_result = 0;

    // Validate attribute number range
    if (fnumber > tupdesc->natts || fnumber == 0 ||
        fnumber <= FirstLowInvalidHeapAttributeNumber) {
        SPI_result = SPI_ERROR_NOATTRIBUTE;
        return InvalidOid;
    }

    // Return type OID from attribute
    if (fnumber > 0) {
        return TupleDescAttr(tupdesc, fnumber - 1)->atttypid;  // Regular attribute
    } else {
        return (SystemAttributeDefinition(fnumber))->atttypid;  // System attribute
    }
}
```