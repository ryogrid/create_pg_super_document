setup_config

## Overview
Creates and configures all PostgreSQL configuration files (postgresql.conf, postgresql.auto.conf, pg_hba.conf, pg_ident.conf) during database cluster initialization.

## Definition
static void setup_config(void)

## Detailed Description
This comprehensive function is responsible for setting up all the essential configuration files required for a PostgreSQL database cluster during initdb. It reads template configuration files and customizes them based on the initialization parameters, system capabilities, and user-specified options.

The function performs several key operations:
1. Sets up postgresql.conf with appropriate values for max_connections, shared_buffers, locale settings, timezone, WAL settings, and various other GUC parameters
2. Creates postgresql.auto.conf as an empty file with warning comments
3. Configures pg_hba.conf with authentication methods and IPv6 support detection
4. Sets up pg_ident.conf for user name mapping

The function automatically determines appropriate values for many settings, such as formatting shared_buffers with optimal units (MB/kB), setting datestyle based on locale, and configuring WAL sizes using pretty_wal_size(). It also handles platform-specific settings and applies any user-specified GUC overrides via command-line options.

## Parameters / Member Variables
This function takes no parameters but operates on numerous global variables including:
- n_connections, n_buffers: Connection and buffer settings
- Various locale variables: lc_messages, lc_monetary, lc_numeric, lc_time
- Authentication method variables: authmethodlocal, authmethodhost
- Configuration file paths: conf_file, hba_file, ident_file
- extra_guc_names, extra_guc_values: User-specified overrides

## Dependencies
- Functions called/Symbols referenced:
  - readfile, writefile
  - replace_guc_value, replace_token
  - pretty_wal_size
  - locale_date_order
  - pg_malloc_array, pg_strdup
  - check_ok
- Called from (representative examples):
  - initialize_data_directory (around line 3079)

## Notes and Other Information
- This is a static function within initdb.c, used specifically during database cluster initialization
- The function handles platform-specific configurations (Windows, IPv6 support detection)
- Automatically formats memory and WAL size values with appropriate units for readability
- Sets appropriate file permissions using pg_file_create_mode
- Includes IPv6 capability detection to avoid runtime warnings on systems without IPv6 support
- Handles special cases like setting password_encryption based on authentication methods
- Supports group access permissions when pg_dir_create_mode is set to PG_DIR_MODE_GROUP
- Template files are read and token replacement is used to customize configuration values