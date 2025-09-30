## Simplified Source

```c
static Datum
get_ts_template_func(DefElem *defel, int attnum)
{
    List *funcName = defGetQualifiedName(defel);
    Oid typeId[4];
    Oid procOid;
    int nargs;

    // Set up function signature - all parameters are INTERNAL type
    Oid retTypeId = INTERNALOID;
    typeId[0] = typeId[1] = typeId[2] = typeId[3] = INTERNALOID;

    // Determine argument count based on template function type
    switch (attnum) {
        case Anum_pg_ts_template_tmplinit:
            nargs = 1;  // Init function takes 1 argument
            break;
        case Anum_pg_ts_template_tmpllexize:
            nargs = 4;  // Lexize function takes 4 arguments
            break;
        default:
            elog(ERROR, "unrecognized attribute for text search template: %d", attnum);
    }

    // Look up the function and validate return type
    procOid = LookupFuncName(funcName, nargs, typeId, false);
    if (get_func_rettype(procOid) != retTypeId) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errmsg("function %s should return type %s",
                              func_signature_string(funcName, nargs, NIL, typeId),
                              format_type_be(retTypeId))));
    }

    return ObjectIdGetDatum(procOid);
}
```