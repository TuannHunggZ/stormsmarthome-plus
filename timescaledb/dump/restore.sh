#!/bin/bash
set -e

echo "Restoring database..."

pg_restore \
    -U postgres \
    -d iotdata \
    --clean \
    --if-exists \
    /backup/iotdata.dump

echo "Restore completed."