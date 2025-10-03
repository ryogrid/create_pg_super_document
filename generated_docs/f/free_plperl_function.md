# free_plperl_function

## Location
[src/pl/plperl/plperl.c:2700-2717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2700-L2717)

## Overview
Frees all resources associated with a PL/Perl function descriptor including Perl code references and PostgreSQL memory contexts.

## Definition

```c
static void
free_plperl_function(plperl_proc_desc *prodesc)
```
## Detailed Description
This function is responsible for the complete cleanup of a PL/Perl function descriptor when its reference count reaches zero. It properly manages both Perl-side and PostgreSQL-side resources. The function activates the appropriate Perl interpreter to safely decrement the Perl code reference, then restores the previous interpreter state. Finally, it deletes the entire memory context associated with the function, which frees all PostgreSQL-allocated memory for the function descriptor.

## Parameters / Member Variables
- `*prodesc`: Pointer to the procedure descriptor structure to be freed
## Dependencies
- Functions called/Symbols referenced:
  - [plperl_proc_desc](../p/plperl_proc_desc.md): Structure type for procedure descriptor
  - [plperl_interp_desc](../p/plperl_interp_desc.md): Structure type for Perl interpreter descriptor
  - [activate_interpreter](../a/activate_interpreter.md): Switches to the specified Perl interpreter
  - [SvREFCNT_dec_current](../S/SvREFCNT_dec_current.md): Decrements Perl scalar reference count
  - [MemoryContextDelete](../M/MemoryContextDelete.md): Deletes PostgreSQL memory context and all contained memory
- Called from:
  - decrement_prodesc_refcount: Called when reference count reaches zero
  - [compile_plperl_function](../c/compile_plperl_function.md): Called during error handling/cleanup

## Notes and Other Information
- Only called when fn_refcount is zero (enforced by Assert)
- Carefully manages Perl interpreter state to ensure proper cleanup
- Handles the case where no Perl code reference exists (prodesc->reference is NULL)
- Deletes the entire memory context, which automatically frees all associated memory
- Part of the reference counting mechanism for PL/Perl function lifecycle management
- Located at src/pl/plperl/plperl.c:2700-2717