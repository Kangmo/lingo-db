#!/bin/bash
set -e

BUILD_TYPE=$1
DB_NAME=$2
ROOT_DIR=$3

DB_PATH="${ROOT_DIR}/build/lingodb-${BUILD_TYPE}/testdata/${DB_NAME}"
SQL_BINARY="${ROOT_DIR}/build/lingodb-${BUILD_TYPE}/sql"
SQL_FILE="${ROOT_DIR}/resources/sql/${DB_NAME}/initialize.sql"

echo "Creating test database '${DB_NAME}' at ${DB_PATH}..."

# Clean and create directory - make sure it's really cleaned
if [ -d "${DB_PATH}" ]; then
    echo "Cleaning existing database directory..."
    rm -rf "${DB_PATH}"
fi

# Wait a moment for filesystem to sync
sleep 0.1

mkdir -p "${DB_PATH}"

# Run SQL with the database path - use wrapper to clean environment
"${ROOT_DIR}/run_sql_safe.sh" "${SQL_BINARY}" "${DB_PATH}" "${SQL_FILE}"
echo "Database created successfully."