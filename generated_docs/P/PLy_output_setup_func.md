# PLy_output_setup_func

## Location
[src/pl/plpython/plpy_typeio.c:296-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L296-L417)

## Overview
PLy_output_setup_func recursively initializes PLyObToDatum structures needed to construct SQL values from Python values, handling all PostgreSQL data types including domains, arrays, transforms, composites, and scalars.

## Definition
```c
void PLy_output_setup_func(PLyObToDatum *arg, MemoryContext arg_mcxt,
                          Oid typeOid, int32 typmod,
                          PLyProcedure *proc)
```

## Detailed Description
This function is the core type conversion setup routine for PL/Python output operations. It performs comprehensive type analysis and configures the appropriate conversion function and data structures based on the PostgreSQL type being handled. The function operates recursively to handle complex nested types.

Key functionality includes:
1. **Type cache lookup**: Retrieves type information from PostgreSQL's type cache system
2. **Type classification**: Determines the type category (domain, array, composite, scalar) and selects appropriate handling
3. **Recursive setup**: For complex types like domains and arrays, recursively sets up conversion for base/element types
4. **Transform support**: Checks for and configures custom transform functions when available
5. **Special case handling**: Provides optimized paths for common types like BOOL and BYTEA

The function handles the RECORD type as a special case, treating it as a composite type without requiring type cache lookups since its structure is indeterminate at this stage.

## Parameters / Member Variables
- `arg`: PLyObToDatum structure to be initialized with conversion information
- `arg_mcxt`: MemoryContext for allocating conversion-related data structures
- `typeOid`: OID of the PostgreSQL type to set up conversion for
- `typmod`: Type modifier providing additional type-specific information
- `proc`: PLyProcedure containing procedure metadata and language-specific information

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Prevents stack overflow in recursive calls
  - [lookup_type_cache](../l/lookup_type_cache.md): Retrieves type information from PostgreSQL's cache
  - [getBaseType](../g/getBaseType.md): Gets the base type for arrays
  - [get_transform_tosql](../g/get_transform_tosql.md): Looks up custom transform functions
  - [getTypeInputInfo](../g/getTypeInputInfo.md): Gets input function information for scalar types
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md): Sets up function manager information
  - Various PLyObject_To* functions: Type-specific conversion functions
- Called from:
  - [PLy_exec_trigger](PLy_exec_trigger.md): Trigger execution setup
  - [PLy_procedure_create](PLy_procedure_create.md): Function procedure creation
  - [PLy_spi_prepare](PLy_spi_prepare.md): SPI statement preparation
  - [PLy_output_setup_tuple](PLy_output_setup_tuple.md): Tuple field setup
  - Itself (recursive calls for complex types)

## Notes and Other Information
- The function is recursive and includes stack depth checking to prevent overflow
- Transform functions are only checked for composite and scalar types, not arrays or domains
- RECORD type handling uses hard-coded type characteristics (not typbyval, length -1, double alignment)
- Memory allocation for nested structures uses the provided memory context for proper cleanup
- Located in src/pl/plpython/plpy_typeio.c at lines 296-417

## Simplified Source

```c
void PLy_output_setup_func(PLyObToDatum *arg, MemoryContext arg_mcxt,
                          Oid typeOid, int32 typmod, PLyProcedure *proc)
{
    check_stack_depth(); // Prevent recursion overflow

    // Initialize basic arg fields
    arg->typoid = typeOid;
    arg->typmod = typmod;
    arg->mcxt = arg_mcxt;

    // Get type information (RECORD is special case)
    if (typeOid != RECORDOID) {
        TypeCacheEntry *typentry = lookup_type_cache(typeOid, TYPECACHE_DOMAIN_BASE_INFO);
        arg->typbyval = typentry->typbyval;
        arg->typlen = typentry->typlen;
        arg->typalign = typentry->typalign;

        // Choose conversion method based on type
        if (typentry->typtype == TYPTYPE_DOMAIN) {
            // Domain: recurse to base type
            arg->func = PLyObject_ToDomain;
            arg->u.domain.base = MemoryContextAllocZero(arg_mcxt, sizeof(PLyObToDatum));
            PLy_output_setup_func(arg->u.domain.base, arg_mcxt,
                                 typentry->domainBaseType, typentry->domainBaseTypmod, proc);
        }
        else if (IsTrueArrayType(typentry)) {
            // Array: recurse to element type
            arg->func = PLySequence_ToArray;
            arg->u.array.elmbasetype = getBaseType(typentry->typelem);
            arg->u.array.elm = MemoryContextAllocZero(arg_mcxt, sizeof(PLyObToDatum));
            PLy_output_setup_func(arg->u.array.elm, arg_mcxt, typentry->typelem, typmod, proc);
        }
        else if (get_transform_tosql(typeOid, proc->langid, proc->trftypes)) {
            // Transform function available
            arg->func = PLyObject_ToTransform;
        }
        else if (typentry->typtype == TYPTYPE_COMPOSITE) {
            // Composite type
            arg->func = PLyObject_ToComposite;
            // Initialize tuple fields
        }
        else {
            // Scalar type with special cases
            switch (typeOid) {
                case BOOLOID:   arg->func = PLyObject_ToBool; break;
                case BYTEAOID:  arg->func = PLyObject_ToBytea; break;
                default:        arg->func = PLyObject_ToScalar; break;
            }
        }
    } else {
        // RECORD type: treat as composite
        arg->typbyval = false;
        arg->typlen = -1;
        arg->typalign = TYPALIGN_DOUBLE;
        arg->func = PLyObject_ToComposite;
    }
}
```