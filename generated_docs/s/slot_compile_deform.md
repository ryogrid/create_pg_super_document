# slot_compile_deform

## Location
[src/backend/jit/llvm/llvmjit_deform.c:34-777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit_deform.c#L34-L777)

## Overview
Compiles an optimized LLVM function that efficiently deforms (unpacks) tuple data from a heap tuple into a TupleTableSlot structure up to a specified number of attributes.

## Definition

```c
struct_gep(b, StructTupleTableSlot, v_slot, FIELDNO_TUPLETABLESLOT_VALUES,
						  "tts_values");
```
## Detailed Description
This function generates optimized LLVM IR code for tuple deformation, which is the process of extracting column values from a heap tuple's binary storage format into the slot's values and nulls arrays. The generated code handles:

- **Virtual tuple optimization**: Returns NULL for virtual tuples that don't need deformation
- **Slot type validation**: Only handles HeapTuple, BufferHeapTuple, and MinimalTuple slot types
- **NULL handling**: Efficiently checks and processes NULL values using the tuple's null bitmap
- **Memory alignment**: Handles column alignment requirements (CHAR, SHORT, INT, DOUBLE)
- **Variable-length data**: Supports fixed-length, variable-length (-1), and null-terminated string (-2) attributes
- **Missing attributes**: Calls slot_getmissingattrs() for tuples with fewer columns than requested
- **Performance optimization**: Generates specialized code paths based on column properties like NOT NULL constraints

The function creates a complex control flow graph with basic blocks for each attribute, handling attribute number checking, null checking, alignment, and data storage. It maintains tracking of known alignment offsets to optimize subsequent column access patterns.

## Parameters / Member Variables
- : LLVM JIT compilation context containing the module and compilation state
- : TupleDesc describing the tuple structure, column types, and attributes
- : TupleTableSlot operations structure identifying the slot type (heap, buffer heap, or minimal tuple)
- : Number of attributes to deform (extract) from the tuple

## Dependencies
- Functions called/Symbols referenced:
  - [llvm_mutable_module](../l/llvm_mutable_module.md): Gets mutable LLVM module for code generation
  - [llvm_expand_funcname](../l/llvm_expand_funcname.md): Creates unique function name
  - [llvm_copy_attributes](../l/llvm_copy_attributes.md): Copies function attributes from template
  - [llvm_pg_func](../l/llvm_pg_func.md): Gets PostgreSQL function declarations
  - [llvm_pg_var_func_type](../l/llvm_pg_var_func_type.md): Gets function type for variable-length operations
  - [slot_getmissingattrs](slot_getmissingattrs.md): Handles missing attributes in sparse tuples
  - [varsize_any](../v/varsize_any.md): Calculates size of variable-length attributes
  - strlen: Calculates length of null-terminated strings
- Called from (representative examples):
  - [llvm_compile_expr](../l/llvm_compile_expr.md): Uses deformation functions in expression compilation

## Notes and Other Information
- The function performs extensive compile-time optimizations based on tuple descriptor analysis
- Generates different code paths for guaranteed NOT NULL columns vs nullable columns
- Maintains alignment tracking to eliminate unnecessary alignment calculations
- Creates a switch-based dispatch system to resume deformation from any previously processed column
- Sets TTS_FLAG_SLOW flag to indicate the slot contains deformed data
- The generated function signature takes a single TupleTableSlot pointer parameter
- Virtual tuples are explicitly excluded as they don't require deformation
- Memory layout assumptions are critical for the generated code's correctness and performance

## Simplified Source

```c
LLVMValueRef
slot_compile_deform(LLVMJitContext *context, TupleDesc desc,
                    const TupleTableSlotOps *ops, int natts)
{
    // Skip virtual tuples - they don't need deformation
    if (ops == &TTSOpsVirtual)
        return NULL;

    // Only handle known slot types
    if (ops != &TTSOpsHeapTuple && ops != &TTSOpsBufferHeapTuple &&
        ops != &TTSOpsMinimalTuple)
        return NULL;

    // Set up LLVM context and function
    LLVMModuleRef mod = llvm_mutable_module(context);
    LLVMContextRef lc = LLVMGetModuleContext(mod);
    char *funcname = llvm_expand_funcname(context, "deform");

    // Create function signature: void deform_func(TupleTableSlot *slot)
    LLVMTypeRef param_types[1] = { l_ptr(StructTupleTableSlot) };
    LLVMTypeRef deform_sig = LLVMFunctionType(LLVMVoidTypeInContext(lc),
                                              param_types, 1, 0);
    LLVMValueRef v_deform_fn = LLVMAddFunction(mod, funcname, deform_sig);

    // Create basic blocks for control flow
    LLVMBasicBlockRef b_entry = LLVMAppendBasicBlockInContext(lc, v_deform_fn, "entry");
    LLVMBasicBlockRef b_out = LLVMAppendBasicBlockInContext(lc, v_deform_fn, "out");

    LLVMBuilderRef b = LLVMCreateBuilderInContext(lc);
    LLVMPositionBuilderAtEnd(b, b_entry);

    // Get slot components
    LLVMValueRef v_slot = LLVMGetParam(v_deform_fn, 0);
    LLVMValueRef v_tts_values = l_load_struct_gep(b, StructTupleTableSlot, v_slot,
                                                  FIELDNO_TUPLETABLESLOT_VALUES, "tts_values");
    LLVMValueRef v_tts_nulls = l_load_struct_gep(b, StructTupleTableSlot, v_slot,
                                                 FIELDNO_TUPLETABLESLOT_ISNULL, "tts_nulls");

    // Get tuple data and header info
    LLVMValueRef v_tupleheaderp = /* load tuple header based on slot type */;
    LLVMValueRef v_tuplep = l_load_struct_gep(b, StructHeapTupleData, v_tupleheaderp,
                                              FIELDNO_HEAPTUPLEDATA_DATA, "tuple");
    LLVMValueRef v_infomask1 = l_load_struct_gep(b, StructHeapTupleHeaderData, v_tuplep,
                                                 FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK, "infomask1");

    // Check if tuple has null values
    LLVMValueRef v_hasnulls = LLVMBuildICmp(b, LLVMIntNE,
                                           LLVMBuildAnd(b, l_int16_const(lc, HEAP_HASNULL),
                                                       v_infomask1, ""),
                                           l_int16_const(lc, 0), "hasnulls");

    // Get tuple data start offset
    LLVMValueRef v_hoff = /* load header offset */;
    LLVMValueRef v_tupdata_base = /* calculate tuple data base address */;
    LLVMValueRef v_offp = LLVMBuildAlloca(b, TypeSizeT, "offset");

    // Process each attribute
    for (int attnum = 0; attnum < natts; attnum++) {
        Form_pg_attribute att = TupleDescAttr(desc, attnum);

        // Check if attribute can be null
        if (!att->attnotnull) {
            // Check null bitmap
            LLVMValueRef v_nullbit = /* check null bit for this attribute */;
            // Branch: if null, store null and continue; else proceed to load data
        }

        // Handle alignment if needed
        if (att->attalign != TYPALIGN_CHAR) {
            // Align offset according to attribute requirements
            LLVMValueRef v_aligned_offset = /* perform alignment calculation */;
            LLVMBuildStore(b, v_aligned_offset, v_offp);
        }

        // Load attribute data
        LLVMValueRef v_attdatap = /* calculate attribute data pointer */;
        LLVMValueRef v_resultp = /* get result storage location */;

        // Store data based on byval/byref
        if (att->attbyval) {
            // Load and zero-extend byval data
            LLVMValueRef v_data = /* load and extend value */;
            LLVMBuildStore(b, v_data, v_resultp);
        } else {
            // Store pointer for byref data
            LLVMValueRef v_ptr = LLVMBuildPtrToInt(b, v_attdatap, TypeSizeT, "ptr");
            LLVMBuildStore(b, v_ptr, v_resultp);
        }

        // Calculate next offset
        LLVMValueRef v_incby;
        if (att->attlen > 0) {
            v_incby = l_sizet_const(att->attlen);
        } else if (att->attlen == -1) {
            v_incby = l_call(b, /* call varsize_any */);
        } else { // att->attlen == -2
            v_incby = l_call(b, /* call strlen and add 1 */);
        }

        // Update offset for next attribute
        LLVMValueRef v_current_off = l_load(b, TypeSizeT, v_offp, "");
        LLVMValueRef v_new_off = LLVMBuildAdd(b, v_current_off, v_incby, "");
        LLVMBuildStore(b, v_new_off, v_offp);
    }

    // Finalize: set nvalid, update slot offset, set flags
    LLVMPositionBuilderAtEnd(b, b_out);
    LLVMValueRef v_nvalidp = l_struct_gep(b, StructTupleTableSlot, v_slot,
                                         FIELDNO_TUPLETABLESLOT_NVALID, "");
    LLVMBuildStore(b, l_int16_const(lc, natts), v_nvalidp);

    // Set slow flag to indicate deformed data
    LLVMValueRef v_flagsp = l_struct_gep(b, StructTupleTableSlot, v_slot,
                                        FIELDNO_TUPLETABLESLOT_FLAGS, "");
    LLVMValueRef v_flags = l_load(b, LLVMInt16TypeInContext(lc), v_flagsp, "");
    v_flags = LLVMBuildOr(b, v_flags, l_int16_const(lc, TTS_FLAG_SLOW), "");
    LLVMBuildStore(b, v_flags, v_flagsp);

    LLVMBuildRetVoid(b);
    LLVMDisposeBuilder(b);

    return v_deform_fn;
}
```