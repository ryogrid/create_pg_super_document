# err_generic_string

## Location
[src/backend/utils/error/elog.c:1512-1547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1512-L1547)

## Overview
A function that sets individual ErrorData string fields identified by PG_DIAG_xxx codes, specifically designed for non-localized string fields to avoid translation considerations.

## Definition
int err_generic_string(int field, const char *str)

## Detailed Description
The err_generic_string function provides a low-level interface for setting specific string fields in PostgreSQL's ErrorData structure. It uses a switch statement to handle different diagnostic field types (PG_DIAG_xxx codes) and delegates the actual field setting to the set_errdata_field helper function. The function intentionally only supports fields that don't require localized strings, ensuring no translation complexities arise. Most callers should prefer higher-level abstractions like errtablecol() rather than calling this function directly.

## Parameters / Member Variables
- field: An integer code (PG_DIAG_xxx constant) identifying which ErrorData field to set
- str: The string value to assign to the specified field

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (structure type)
  - CHECK_STACK_DEPTH (macro for stack validation)
  - [set_errdata_field](../s/set_errdata_field.md) (helper function for field assignment)
  - PG_DIAG_SCHEMA_NAME, PG_DIAG_TABLE_NAME, PG_DIAG_COLUMN_NAME, PG_DIAG_DATATYPE_NAME, PG_DIAG_CONSTRAINT_NAME (diagnostic field constants)
  - elog (for error reporting on unsupported fields)
- Called from (representative examples):
  - [errdatatype](errdatatype.md) (in domains.c)
  - [errdomainconstraint](errdomainconstraint.md) (in domains.c)
  - [errtable](errtable.md) (in relcache.c)
  - [errtablecolname](errtablecolname.md) (in relcache.c)
  - [errtableconstraint](errtableconstraint.md) (in relcache.c)
  - [PLy_elog_impl](../P/PLy_elog_impl.md) (in plpy_elog.c)
  - [PLy_output](../P/PLy_output.md) (in plpy_plpymodule.c)

## Notes and Other Information
- The function always returns 0, indicating the return value is not significant
- Supports only a specific set of diagnostic fields: schema, table, column, datatype, and constraint names
- Throws an ERROR if an unsupported field ID is provided
- Does not increment recursion_depth, unlike some other error functions
- Located in src/backend/utils/error/elog.c:1512-1547
- Part of PostgreSQL's error reporting infrastructure for providing structured diagnostic information

## Simplified Source

```c
int err_generic_string(int field, const char *str) {
    ErrorData *edata = &errordata[errordata_stack_depth];

    // Validate stack depth
    CHECK_STACK_DEPTH();

    // Set the appropriate field based on diagnostic code
    switch (field) {
        case PG_DIAG_SCHEMA_NAME:
            set_errdata_field(edata->assoc_context, &edata->schema_name, str);
            break;
        case PG_DIAG_TABLE_NAME:
            set_errdata_field(edata->assoc_context, &edata->table_name, str);
            break;
        case PG_DIAG_COLUMN_NAME:
            set_errdata_field(edata->assoc_context, &edata->column_name, str);
            break;
        case PG_DIAG_DATATYPE_NAME:
            set_errdata_field(edata->assoc_context, &edata->datatype_name, str);
            break;
        case PG_DIAG_CONSTRAINT_NAME:
            set_errdata_field(edata->assoc_context, &edata->constraint_name, str);
            break;
        default:
            elog(ERROR, "unsupported ErrorData field id: %d", field);
            break;
    }

    return 0;  // Return value not significant
}
```