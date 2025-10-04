# plperl_destroy_interp

## Location
[src/pl/plperl/plperl.c:918-956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L918-L956)

## Overview
Performs controlled cleanup of a Perl interpreter by running END blocks while avoiding full destruction that could impact PostgreSQL's stability.

## Definition

```c
struction is performed: - just call END
		 * blocks.
		 *
		 * We could call perl_destruct() but we'd need to audit its actions
		 * very carefully and work-around any that impact us. (Calling
		 * sv_clean_objs() isn't an option because it's not part of perl's
		 * public API so isn't portably available.) Meanwhile END blocks can
		 * be used to perform manual cleanup.
		 */
		dTHX;
```
## Detailed Description
The  function implements a minimal but safe cleanup strategy for Perl interpreters in the PL/Perl environment. Rather than performing a full perl_destruct() which could have unpredictable side effects on PostgreSQL's process state, this function takes a conservative approach by only running Perl END blocks.

The function's design philosophy prioritizes PostgreSQL's stability over complete Perl cleanup. It:
- Runs END blocks to allow Perl code to perform manual cleanup
- Avoids calling perl_destruct() due to potential adverse effects
- Uses Perl's exception handling mechanism (JMPENV) to safely execute END blocks
- Performs basic cleanup of temporaries and stack state
- Sets the interpreter pointer to NULL to prevent reuse

This approach allows Perl modules and user code to clean up resources through END blocks while maintaining the integrity of the PostgreSQL process.

## Parameters / Member Variables
- : Double pointer to PerlInterpreter instance; allows the function to nullify the pointer after cleanup

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl macro for threading context)
  - dJMPENV (Perl macro for exception handling setup)
  - JMPENV_PUSH/JMPENV_POP (Perl exception handling macros)
  - call_list (Perl function to execute array of subroutines)
  - LEAVE (Perl macro for scope cleanup)
  - FREETMPS (Perl macro for temporary cleanup)
  - PERL_UNUSED_VAR (Perl macro to suppress warnings)
  - PL_exit_flags, PL_endav, PL_minus_c, PL_scopestack_ix (Perl global variables)
- Called from (representative examples):
  - [plperl_fini](plperl_fini.md) (during process cleanup)

## Notes and Other Information
- Implements minimal destruction strategy to avoid perl_destruct() side effects
- Uses exception handling to safely run END blocks without crashing PostgreSQL
- Conservative approach prioritizes PostgreSQL stability over complete cleanup
- Allows Perl code to perform manual resource cleanup through END blocks
- Part of PL/Perl's safe shutdown procedure during process termination
- Designed to be called only when the target interpreter is the active one
- Sets interpreter pointer to NULL to prevent accidental reuse after destruction
- Based on Perl's own perl_destruct() implementation but significantly simplified

## Simplified Source

```c
static void
plperl_destroy_interp(PerlInterpreter **interp)
{
    if (interp && *interp)
    {
        dTHX;

        // Run END blocks safely using Perl's exception handling
        if (PL_exit_flags & PERL_EXIT_DESTRUCT_END)
        {
            dJMPENV;
            int x = 0;

            JMPENV_PUSH(x);
            PERL_UNUSED_VAR(x);
            if (PL_endav && !PL_minus_c)
                call_list(PL_scopestack_ix, PL_endav);
            JMPENV_POP;
        }

        // Clean up temporaries and scope
        LEAVE;
        FREETMPS;

        // Mark interpreter as destroyed
        *interp = NULL;
    }
}
```