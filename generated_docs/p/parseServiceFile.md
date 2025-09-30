# parseServiceFile

## Location
[src/interfaces/libpq/fe-connect.c:5560-5737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5560-L5737)

## Overview
Parses a PostgreSQL service configuration file to extract connection parameters for a specified service name.

## Definition
```c
static int parseServiceFile(const char *serviceFile,
                           const char *service,
                           PQconninfoOption *options,
                           PQExpBuffer errorMessage,
                           bool *group_found)
```

## Detailed Description
This function reads and parses a PostgreSQL service configuration file (typically ~/.pg_service.conf or system-wide configuration) to extract connection parameters for a named service. The service file uses an INI-like format with sections enclosed in square brackets and key=value pairs within each section. The function locates the specified service section and populates the provided options array with the configuration values found.

The function handles various parsing scenarios including:
- File access and validation
- Line-by-line parsing with proper whitespace handling
- Service section identification using bracket notation [servicename]
- Key=value pair extraction and validation
- LDAP service lookup integration (when compiled with USE_LDAP)
- Error reporting with detailed context information

## Parameters / Member Variables
- `serviceFile`: Path to the service configuration file to parse
- `service`: Name of the service section to locate and parse
- `options`: Array of PQconninfoOption structures to populate with parsed values
- `errorMessage`: Buffer for storing detailed error messages if parsing fails
- `group_found`: Pointer to boolean flag indicating whether the specified service section was found

## Dependencies
- Functions called/Symbols referenced:
  - fopen
  - [libpq_append_error](../l/libpq_append_error.md)
  - [ldapServiceLookup](../l/ldapServiceLookup.md) (when USE_LDAP is defined)
  - [PQconninfoOption](../P/PQconninfoOption.md) (data structure)
- Called from (representative examples):
  - [parseServiceInfo](parseServiceInfo.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns 0 on success, 1 if file not found, 2 if line too long, 3 on syntax/memory errors
- The function does not override previously set option values, allowing for parameter precedence
- Supports LDAP service lookup as a fallback mechanism when compiled with LDAP support
- Uses a 1024-byte buffer for line reading, enforcing a maximum line length limit
- Nested service specifications are explicitly forbidden and result in error
- The parser ignores comments (lines starting with #) and empty lines
- Leading and trailing whitespace is automatically trimmed from each line

## Simplified Source

```c
static int parseServiceFile(const char *serviceFile, const char *service,
                           PQconninfoOption *options, PQExpBuffer errorMessage,
                           bool *group_found) {
    FILE *file;
    char line_buffer[1024];
    char *line;
    int line_number = 0;
    bool in_target_section = false;
    *group_found = false;

    // Open service file
    file = fopen(serviceFile, "r");
    if (!file) {
        libpq_append_error(errorMessage, "service file \"%s\" not found", serviceFile);
        return 1;
    }

    // Process each line
    while ((line = fgets(line_buffer, sizeof(line_buffer), file)) != NULL) {
        line_number++;

        // Check for overly long lines
        if (strlen(line) >= sizeof(line_buffer) - 1) {
            libpq_append_error(errorMessage, "line %d too long in service file \"%s\"",
                             line_number, serviceFile);
            fclose(file);
            return 2;
        }

        // Trim whitespace
        int len = strlen(line);
        while (len > 0 && isspace(line[len - 1])) line[--len] = '\0';
        while (*line && isspace(*line)) line++;

        // Skip empty lines and comments
        if (line[0] == '\0' || line[0] == '#') continue;

        // Handle section headers [servicename]
        if (line[0] == '[') {
            if (in_target_section) {
                // Found end of our section
                break;
            }
            // Check if this is our target section
            if (strncmp(line + 1, service, strlen(service)) == 0 &&
                line[strlen(service) + 1] == ']') {
                in_target_section = true;
                *group_found = true;
            } else {
                in_target_section = false;
            }
        }
        // Handle key=value pairs within target section
        else if (in_target_section) {
            char *key = line;
            char *value = strchr(line, '=');

            if (!value) {
                libpq_append_error(errorMessage, "syntax error in service file \"%s\", line %d",
                                 serviceFile, line_number);
                fclose(file);
                return 3;
            }

            *value++ = '\0';  // Split key and value

            // Reject nested service specifications
            if (strcmp(key, "service") == 0) {
                libpq_append_error(errorMessage,
                    "nested service specifications not supported in service file \"%s\", line %d",
                    serviceFile, line_number);
                fclose(file);
                return 3;
            }

            // Store the parameter value if it's a valid option
            bool found_option = false;
            for (int i = 0; options[i].keyword; i++) {
                if (strcmp(options[i].keyword, key) == 0) {
                    if (options[i].val == NULL) {  // Don't override existing values
                        options[i].val = strdup(value);
                        if (!options[i].val) {
                            libpq_append_error(errorMessage, "out of memory");
                            fclose(file);
                            return 3;
                        }
                    }
                    found_option = true;
                    break;
                }
            }

            if (!found_option) {
                libpq_append_error(errorMessage, "syntax error in service file \"%s\", line %d",
                                 serviceFile, line_number);
                fclose(file);
                return 3;
            }
        }
    }

    fclose(file);
    return 0;  // Success
}
```