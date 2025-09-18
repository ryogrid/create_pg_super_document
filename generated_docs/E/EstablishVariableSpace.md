# EstablishVariableSpace

## Location
src/bin/psql/startup.c: 1198 - 1268

## Overview
Initializes the psql variable space and sets up all configuration variable hooks for psql session management.

## Definition
```c
static void EstablishVariableSpace(void)
```

## Detailed Description
This function is the central initialization point for psql's configuration variable system. It creates the variable space using CreateVariableSpace() and then systematically registers all psql configuration variables with their corresponding substitute and validation hooks. Each variable is configured with appropriate hook functions that handle default value substitution and value validation/assignment. This comprehensive setup enables psql's flexible configuration system that allows users to customize behavior through various settings.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CreateVariableSpace (creates the variable storage system)
  - SetVariableHooks (registers hooks for each variable)
  - bool_substitute_hook (default substitute hook for boolean variables)
  - autocommit_hook, on_error_stop_hook, quiet_hook, singleline_hook, singlestep_hook
  - fetch_count_substitute_hook, fetch_count_hook
  - histfile_hook, histsize_substitute_hook, histsize_hook
  - ignoreeof_substitute_hook, ignoreeof_hook
  - echo_substitute_hook, echo_hook, echo_hidden_hook
  - on_error_rollback_hook, comp_keyword_case_substitute_hook, comp_keyword_case_hook
  - histcontrol_substitute_hook, histcontrol_hook
  - prompt1_hook, prompt2_hook, prompt3_hook
  - verbosity_substitute_hook, verbosity_hook
  - show_all_results_hook, show_context_substitute_hook, show_context_hook
  - hide_compression_hook, hide_tableam_hook
- Called from (representative examples):
  - adhoc_opts (at src/bin/psql/startup.c:87)
  - main (at src/bin/psql/startup.c:190)

## Notes and Other Information
- This is a static function defined in src/bin/psql/startup.c
- Called early in psql startup to establish the complete configuration system
- Registers hooks for all major psql configuration variables including:
  - Boolean settings (AUTOCOMMIT, ON_ERROR_STOP, QUIET, etc.)
  - Numeric settings (FETCH_COUNT, HISTSIZE, IGNOREEOF)
  - String settings (HISTFILE, ECHO, COMP_KEYWORD_CASE, HISTCONTROL)
  - Prompt settings (PROMPT1, PROMPT2, PROMPT3)
  - Display settings (VERBOSITY, SHOW_CONTEXT, HIDE_TOAST_COMPRESSION, HIDE_TABLEAM)
- Each SetVariableHooks call associates a variable name with substitute and validation hook functions
- Some variables use NULL for substitute hooks when no default substitution is needed
- Part of psql's modular configuration architecture that separates variable storage from validation logic