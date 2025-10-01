# get_op_opfamily_sortfamily

## Location
[src/backend/utils/cache/lsyscache.c:108-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L108-L135)

## Overview
Retrieves the sort family OID associated with an ordering operator within a specified operator family, or returns InvalidOid if the operator is not an ordering operator in that family.

## Definition

```c
Oid
get_op_opfamily_sortfamily(Oid opno, Oid opfamily)
```
## Detailed Description
This function looks up an operator in the pg_amop system catalog to determine if it is an ordering operator within the given operator family, and if so, returns the associated sort family OID. Unlike the previous functions, this specifically searches for ordering operators (AMOP_ORDER) rather than search operators. Sort families define how operators sort data, which is essential for operations like ORDER BY clauses and certain index operations. If the operator is not found as an ordering operator in the specified family, InvalidOid is returned.

## Parameters / Member Variables
- : The OID of the operator to look up
- : The OID of the operator family to search within

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache3](../S/SearchSysCache3.md) (system cache lookup function)
  - HeapTupleIsValid (checks if tuple is valid)
  - GETSTRUCT (extracts structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases cache reference)
  - Form_pg_amop (structure type for pg_amop catalog)
  - AMOP_ORDER (constant for ordering operator type)
  - [CharGetDatum](../C/CharGetDatum.md) (datum conversion function)
  - InvalidOid (constant representing invalid OID)
- Called from (representative examples):
  - [match_clause_to_ordering_op](../m/match_clause_to_ordering_op.md) (src/backend/optimizer/path/indxpath.c:3194)

## Notes and Other Information
- Specifically searches for ordering operators (AMOP_ORDER) rather than search operators (AMOP_SEARCH)
- [Sort](../S/Sort.md) families define the collation and ordering behavior for operators, crucial for sorting operations
- Returns InvalidOid when the operator is not found as an ordering operator in the family
- The amopsortfamily field may be InvalidOid even for valid ordering operators if no specific sort family is defined
- Used primarily in query optimization for ORDER BY clause processing and index scan ordering
- Ordering operators are typically used for < <= > >= comparisons in sorting contexts rather than equality testing

## Simplified Source

```c
Oid
get_op_opfamily_sortfamily(Oid opno, Oid opfamily)
{
    HeapTuple tp;
    Form_pg_amop amop_tup;
    Oid result;

    // Search pg_amop catalog for ordering operator in specified family
    tp = SearchSysCache3(AMOPOPID,
                        ObjectIdGetDatum(opno),
                        CharGetDatum(AMOP_ORDER),
                        ObjectIdGetDatum(opfamily));

    if (!HeapTupleIsValid(tp))
        return InvalidOid;

    // Extract sort family OID from the catalog entry
    amop_tup = (Form_pg_amop) GETSTRUCT(tp);
    result = amop_tup->amopsortfamily;
    ReleaseSysCache(tp);

    return result;
}
```