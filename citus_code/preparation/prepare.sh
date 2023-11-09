#!/bin/bash
psql <<EOF
\i ./preparation/delete_tables.sql
\i ./preparation/data_modeling.sql
\i ./preparation/data_import.sql
EOF