# get_opclass_name

## Location
[src/backend/utils/adt/ruleutils.c:12531-12568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12531-L12568)

## Overview
Fetches the name of an index operator class and appends it to a string buffer, with automatic suppression when the opclass is the default for the given data type.

## Definition

```c
static void
get_opclass_name(Oid opclass, Oid actual_datatype,
				 StringInfo buf)
```
## Detailed Description
This function retrieves the name of an operator class from the system catalog and conditionally appends it to the provided string buffer. The function implements smart formatting behavior by suppressing output when the specified operator class is the default for the actual data type, helping to keep generated SQL statements clean and readable. When the operator class name is needed, the function properly handles namespace qualification based on visibility rules.

The function performs a system catalog lookup to retrieve operator class information, checks if the operator class is the default for the given data type, and formats the output with appropriate schema qualification when necessary.

## Parameters / Member Variables
- : The OID of the operator class to look up
- : The OID of the actual data type; if InvalidOid, suppression logic is bypassed
- : StringInfo buffer where the operator class name will be appended (after a space)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_opclass (system catalog form)
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md) (determines default operator class for data type)
  - [OpclassIsVisible](../O/OpclassIsVisible.md) (checks operator class visibility)
  - [quote_identifier](../q/quote_identifier.md) (properly quotes identifiers)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (retrieves namespace name)
- Called from (representative examples):
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_partkeydef_worker](../p/pg_get_partkeydef_worker.md)
  - [get_rule_expr](get_rule_expr.md)
  - [generate_opclass_name](generate_opclass_name.md)

## Notes and Other Information
- Output is suppressed when the opclass is the default for the actual_datatype to keep SQL output clean
- Automatically handles schema qualification based on search path visibility
- Uses system cache lookups for efficient operator class information retrieval
- Part of the rule decompilation system used for displaying index definitions and other SQL constructs

## Simplified Source

```c
static void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf) {
    HeapTuple ht_opc;
    Form_pg_opclass opcrec;
    char *opcname;
    char *nspname;

    // Look up operator class in system cache
    ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(ht_opc))
        elog(ERROR, "cache lookup failed for opclass %u", opclass);
    opcrec = (Form_pg_opclass) GETSTRUCT(ht_opc);

    // Only output name if not the default for this data type
    if (!OidIsValid(actual_datatype) ||
        GetDefaultOpClass(actual_datatype, opcrec->opcmethod) != opclass) {

        opcname = NameStr(opcrec->opcname);

        // Add schema qualification if needed
        if (OpclassIsVisible(opclass))
            appendStringInfo(buf, " %s", quote_identifier(opcname));
        else {
            nspname = get_namespace_name_or_temp(opcrec->opcnamespace);
            appendStringInfo(buf, " %s.%s",
                             quote_identifier(nspname),
                             quote_identifier(opcname));
        }
    }
    ReleaseSysCache(ht_opc);
}
```