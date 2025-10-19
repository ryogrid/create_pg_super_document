# EstablishVariableSpace

## Location
[src/bin/psql/startup.c:1198-1268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1198-L1268)

## Overview
Initializes the psql variable space and sets up all configuration variable hooks for psql session management.

## Definition
```c
static void EstablishVariableSpace(void)
```

## Detailed Description
This function is the central initialization point for psql's configuration variable system. It creates the variable space using CreateVariableSpace() and then systematically registers all psql configuration variables with their corresponding substitute and validation hooks. Each variable is configured with appropriate hook functions that handle default value substitution and value validation/assignment. This comprehensive setup enables psql's flexible configuration system that allows users to customize behavior through various settings.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [CreateVariableSpace](../C/CreateVariableSpace.md) (creates the variable storage system)
  - [SetVariableHooks](../S/SetVariableHooks.md) (registers hooks for each variable)
  - [bool_substitute_hook](../b/bool_substitute_hook.md) (default substitute hook for boolean variables)
  - [autocommit_hook](../a/autocommit_hook.md), on_error_stop_hook, quiet_hook, singleline_hook, singlestep_hook
  - [fetch_count_substitute_hook](../f/fetch_count_substitute_hook.md), fetch_count_hook
  - [histfile_hook](../h/histfile_hook.md), histsize_substitute_hook, histsize_hook
  - [ignoreeof_substitute_hook](../i/ignoreeof_substitute_hook.md), ignoreeof_hook
  - [echo_substitute_hook](../e/echo_substitute_hook.md), echo_hook, echo_hidden_hook
  - [on_error_rollback_hook](../o/on_error_rollback_hook.md), comp_keyword_case_substitute_hook, comp_keyword_case_hook
  - [histcontrol_substitute_hook](../h/histcontrol_substitute_hook.md), histcontrol_hook
  - [prompt1_hook](../p/prompt1_hook.md), prompt2_hook, prompt3_hook
  - [verbosity_substitute_hook](../v/verbosity_substitute_hook.md), verbosity_hook
  - [show_all_results_hook](../s/show_all_results_hook.md), show_context_substitute_hook, show_context_hook
  - [hide_compression_hook](../h/hide_compression_hook.md), hide_tableam_hook
- Called from (representative examples):
  - [adhoc_opts](../a/adhoc_opts.md) (at src/bin/psql/startup.c:87)
  - [main](../m/main.md) (at src/bin/psql/startup.c:190)

## Notes and Other Information
- This is a static function defined in src/bin/psql/startup.c
- Called early in psql startup to establish the complete configuration system
- Registers hooks for all major psql configuration variables including:
  - [Boolean](../B/Boolean.md) settings (AUTOCOMMIT, ON_ERROR_STOP, QUIET, etc.)
  - [Numeric](../N/Numeric.md) settings (FETCH_COUNT, HISTSIZE, IGNOREEOF)
  - [String](../S/String.md) settings (HISTFILE, ECHO, COMP_KEYWORD_CASE, HISTCONTROL)
  - Prompt settings (PROMPT1, PROMPT2, PROMPT3)
  - Display settings (VERBOSITY, SHOW_CONTEXT, HIDE_TOAST_COMPRESSION, HIDE_TABLEAM)
- Each SetVariableHooks call associates a variable name with substitute and validation hook functions
- Some variables use NULL for substitute hooks when no default substitution is needed
- Part of psql's modular configuration architecture that separates variable storage from validation logic

## Simplified Source

```c
static void EstablishVariableSpace(void) {
    // Create the variable storage system
    pset.vars = CreateVariableSpace();

    // Set up hooks for boolean configuration variables
    SetVariableHooks(pset.vars, "AUTOCOMMIT", bool_substitute_hook, autocommit_hook);
    SetVariableHooks(pset.vars, "ON_ERROR_STOP", bool_substitute_hook, on_error_stop_hook);
    SetVariableHooks(pset.vars, "QUIET", bool_substitute_hook, quiet_hook);
    SetVariableHooks(pset.vars, "SINGLELINE", bool_substitute_hook, singleline_hook);
    SetVariableHooks(pset.vars, "SINGLESTEP", bool_substitute_hook, singlestep_hook);
    SetVariableHooks(pset.vars, "ECHO_HIDDEN", bool_substitute_hook, echo_hidden_hook);
    SetVariableHooks(pset.vars, "ON_ERROR_ROLLBACK", bool_substitute_hook, on_error_rollback_hook);
    SetVariableHooks(pset.vars, "SHOW_ALL_RESULTS", bool_substitute_hook, show_all_results_hook);
    SetVariableHooks(pset.vars, "HIDE_TOAST_COMPRESSION", bool_substitute_hook, hide_compression_hook);
    SetVariableHooks(pset.vars, "HIDE_TABLEAM", bool_substitute_hook, hide_tableam_hook);

    // Set up hooks for numeric/special variables
    SetVariableHooks(pset.vars, "FETCH_COUNT", fetch_count_substitute_hook, fetch_count_hook);
    SetVariableHooks(pset.vars, "HISTSIZE", histsize_substitute_hook, histsize_hook);
    SetVariableHooks(pset.vars, "IGNOREEOF", ignoreeof_substitute_hook, ignoreeof_hook);

    // Set up hooks for string variables
    SetVariableHooks(pset.vars, "HISTFILE", NULL, histfile_hook);
    SetVariableHooks(pset.vars, "ECHO", echo_substitute_hook, echo_hook);
    SetVariableHooks(pset.vars, "COMP_KEYWORD_CASE", comp_keyword_case_substitute_hook, comp_keyword_case_hook);
    SetVariableHooks(pset.vars, "HISTCONTROL", histcontrol_substitute_hook, histcontrol_hook);
    SetVariableHooks(pset.vars, "VERBOSITY", verbosity_substitute_hook, verbosity_hook);
    SetVariableHooks(pset.vars, "SHOW_CONTEXT", show_context_substitute_hook, show_context_hook);

    // Set up prompt variables
    SetVariableHooks(pset.vars, "PROMPT1", NULL, prompt1_hook);
    SetVariableHooks(pset.vars, "PROMPT2", NULL, prompt2_hook);
    SetVariableHooks(pset.vars, "PROMPT3", NULL, prompt3_hook);
}
```