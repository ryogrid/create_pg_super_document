# prompt_for_password

## Location
[src/bin/psql/command.c:3335-3362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3335-L3362)

## Overview
Prompts the user for a password with proper signal handling and optional username context, returning a malloc'd string.

## Definition
```c
static char *prompt_for_password(const char *username, bool *canceled)
```

## Detailed Description
This function provides a secure password input mechanism for psql with proper signal handling capabilities. It uses the simple_prompt_extended function to collect password input while hiding the typed characters. The function sets up a prompt interrupt context to handle SIGINT signals gracefully, allowing users to cancel the password prompt. It customizes the prompt message based on whether a username is provided, showing either a generic "Password: " prompt or a user-specific "Password for user %s: " prompt.

## Parameters / Member Variables
- `username`: The username for which the password is being requested (can be NULL or empty)
- `canceled`: Optional output parameter that indicates if the prompt was canceled via SIGINT

## Dependencies
- Functions called/Symbols referenced:
  - [simple_prompt_extended](../s/simple_prompt_extended.md)
  - [psprintf](psprintf.md)
  - free
  - [PromptInterruptContext](../P/PromptInterruptContext.md) (struct)
- Called from (representative examples):
  - [do_connect](../d/do_connect.md)

## Notes and Other Information
- This is a static function used internally within psql's connection handling
- Returns a malloc'd string that must be freed by the caller
- Properly handles SIGINT interruption through PromptInterruptContext
- Uses internationalization with _() macro for the prompt text
- The password input is hidden from display for security
- Part of psql's connection establishment and authentication infrastructure
- Sets up signal handling to allow graceful cancellation of password prompts

## Simplified Source

```c
static char *prompt_for_password(const char *username, bool *canceled) {
    char *result;
    PromptInterruptContext prompt_ctx;

    // Set up signal handling for SIGINT cancellation
    prompt_ctx.jmpbuf = sigint_interrupt_jmp;
    prompt_ctx.enabled = &sigint_interrupt_enabled;
    prompt_ctx.canceled = false;

    // Choose appropriate prompt message
    if (username == NULL || username[0] == '\0') {
        result = simple_prompt_extended("Password: ", false, &prompt_ctx);
    } else {
        char *prompt_text = psprintf(_("Password for user %s: "), username);
        result = simple_prompt_extended(prompt_text, false, &prompt_ctx);
        free(prompt_text);
    }

    // Return cancellation status if requested
    if (canceled)
        *canceled = prompt_ctx.canceled;

    return result;
}
```