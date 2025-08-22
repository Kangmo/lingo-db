#!/usr/bin/env bash
# Wrapper script to run SQL safely

SQL_BINARY="$1"
DB_PATH="$2"
SQL_FILE="$3"

# Clear any potentially problematic environment variables
unset MAKEFLAGS
unset MFLAGS
unset MAKELEVEL

# Run the SQL binary
exec /usr/bin/env -i PATH="$PATH" HOME="$HOME" "${SQL_BINARY}" "${DB_PATH}" < "${SQL_FILE}"