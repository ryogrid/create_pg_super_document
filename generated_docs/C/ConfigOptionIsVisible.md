# ConfigOptionIsVisible

## Location
[src/backend/utils/misc/guc_funcs.c:581-593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L581-L593)

## Overview
ConfigOptionIsVisible determines whether a GUC (Grand Unified Configuration) parameter should be visible to the current user based on security permissions and parameter flags.

## Definition
bool ConfigOptionIsVisible(struct config_generic *conf)

## Detailed Description
This function implements access control for GUC configuration parameters by checking whether the current user has sufficient privileges to view a particular parameter. The visibility logic is straightforward:

1. **Superuser-Only Check**: If the parameter has the GUC_SUPERUSER_ONLY flag set, the function checks whether the current user has the privileges of the pg_read_all_settings role
2. **Permission Verification**: Uses has_privs_of_role() to determine if the current user (obtained via GetUserId()) has the privileges associated with ROLE_PG_READ_ALL_SETTINGS
3. **Access Decision**: Returns false if the parameter is restricted and the user lacks proper privileges; otherwise returns true

The function serves as a security gate for sensitive configuration parameters that should only be visible to privileged users, such as security-related settings, file paths, or other potentially sensitive configuration values.

## Parameters / Member Variables
- conf: Pointer to the config_generic structure representing the GUC parameter to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [has_privs_of_role](../h/has_privs_of_role.md) (to check if current user has privileges of a specific role)
  - [GetUserId](../G/GetUserId.md) (implicitly called through has_privs_of_role to get current user ID)
  - GUC_SUPERUSER_ONLY (flag constant identifying restricted parameters)
  - ROLE_PG_READ_ALL_SETTINGS (predefined role OID for configuration access)
  - [config_generic](../c/config_generic.md) (structure type for GUC parameters)
- Called from (representative examples):
  - [ShowAllGUCConfig](../S/ShowAllGUCConfig.md) (to filter visible parameters in SHOW ALL)
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md) (to check access before returning parameter values)
  - [show_all_settings](../s/show_all_settings.md) (in pg_settings system view)
  - [get_explain_guc_options](../g/get_explain_guc_options.md) (for EXPLAIN command parameter display)

## Notes and Other Information
- This function is central to PostgreSQL security model for configuration parameter access
- The GUC_SUPERUSER_ONLY flag (0x000400) marks parameters that require elevated privileges
- The pg_read_all_settings role provides a way to grant configuration viewing privileges without full superuser access
- Returns true by default for non-restricted parameters, ensuring normal parameters remain visible to all users
- Used throughout the GUC system to maintain consistent access control across different interfaces
- Located in src/backend/utils/misc/guc_funcs.c:581-593