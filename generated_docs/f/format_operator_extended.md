# format_operator_extended

## Location
[src/backend/utils/adt/regproc.c:722-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L722-L792)

## Overview
Converts an operator OID to a textual representation in the format "opr_name(args)", providing extended formatting options and error handling capabilities.

## Definition

```c
char *
format_operator_extended(Oid operator_oid, bits16 flags)
```
## Detailed Description
The  function is the core operator formatting function that converts an operator OID to its textual representation. It provides the underlying functionality used by other operator formatting functions and offers flexible control over the output format through flag parameters.

The function queries the pg_operator system catalog to retrieve operator information and constructs a string in the format "opr_name(lefttype,righttype)" or "opr_name(NONE,righttype)" for unary operators. It handles schema qualification based on visibility rules and flag settings, and can optionally return NULL for invalid operator OIDs.

## Parameters / Member Variables
- : The OID of the operator to format
- : Bit flags controlling the formatting behavior:
  - : Return NULL for invalid/unknown operator OIDs instead of numeric representation
  - : Always include schema qualification regardless of search_path visibility

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - IsBootstrapProcessingMode
  - [initStringInfo](../i/initStringInfo.md)
  - [OperatorIsVisible](../O/OperatorIsVisible.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [format_type_be_qualified](format_type_be_qualified.md)
  - [format_type_be](format_type_be.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [palloc](../p/palloc.md)
  - snprintf
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md) (src/backend/catalog/objectaddress.c:3143)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md) (src/backend/catalog/objectaddress.c:5037)
  - [format_operator](format_operator.md) (src/backend/utils/adt/regproc.c:795)
  - [format_operator_qualified](format_operator_qualified.md) (src/backend/utils/adt/regproc.c:801)

## Notes and Other Information
- This is the primary implementation function for operator formatting in PostgreSQL
- Handles both binary and unary operators, representing missing operands as "NONE"
- Uses StringInfo buffer for efficient string construction
- Includes schema qualification logic based on operator visibility in the current search_path
- Bootstrap mode is not supported (assertion check included)
- Returns a palloc'd string that must be freed by the caller
- For invalid OIDs, either returns NULL or the numeric OID as a string depending on flags
- Located in src/backend/utils/adt/regproc.c:722-792

## Simplified Source

```c
char *format_operator_extended(Oid operator_oid, bits16 flags) {
    // Look up operator in system catalog
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operator_oid));

    if (HeapTupleIsValid(opertup)) {
        Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);
        char *oprname = NameStr(operform->oprname);
        StringInfoData buf;

        initStringInfo(&buf);

        // Add schema qualification if needed
        if ((flags & FORMAT_OPERATOR_FORCE_QUALIFY) != 0 ||
            !OperatorIsVisible(operator_oid)) {
            char *nspname = get_namespace_name(operform->oprnamespace);
            appendStringInfo(&buf, "%s.", quote_identifier(nspname));
        }

        // Build operator signature: name(lefttype,righttype)
        appendStringInfo(&buf, "%s(", oprname);

        // Left operand type (or NONE for unary operators)
        if (operform->oprleft) {
            char *lefttype = (flags & FORMAT_OPERATOR_FORCE_QUALIFY) ?
                format_type_be_qualified(operform->oprleft) :
                format_type_be(operform->oprleft);
            appendStringInfo(&buf, "%s,", lefttype);
        } else {
            appendStringInfoString(&buf, "NONE,");
        }

        // Right operand type (or NONE for prefix operators)
        if (operform->oprright) {
            char *righttype = (flags & FORMAT_OPERATOR_FORCE_QUALIFY) ?
                format_type_be_qualified(operform->oprright) :
                format_type_be(operform->oprright);
            appendStringInfo(&buf, "%s)", righttype);
        } else {
            appendStringInfoString(&buf, "NONE)");
        }

        ReleaseSysCache(opertup);
        return buf.data;
    }

    // Handle invalid operator OID
    if ((flags & FORMAT_OPERATOR_INVALID_AS_NULL) != 0) {
        return NULL;  // Return NULL for missing operators
    } else {
        // Return numeric OID as fallback
        char *result = palloc(NAMEDATALEN);
        snprintf(result, NAMEDATALEN, "%u", operator_oid);
        return result;
    }
}
```