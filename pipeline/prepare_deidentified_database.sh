#!/bin/bash

# Create a dump of the schema
pg_dump --dbname=CVD-Net --schema=cvdnet_consolidated --file=db_dump.sql

# Give the current version of the schema a temporary rename
psql -c 'ALTER SCHEMA cvdnet_consolidated RENAME TO cvdnet_consolidated_temp' CVD-Net

# Load in the dump
psql -f db_dump.sql CVD-Net

# Rename the loaded dump to have '_deidentified'
psql -c 'ALTER SCHEMA cvdnet_consolidated RENAME TO cvdnet_consolidated_deidentified' CVD-Net

# Change the temporary renamed schema to the original schema name
psql -c 'ALTER SCHEMA cvdnet_consolidated_temp RENAME TO cvdnet_consolidated' CVD-Net

# Drop the non-deidentified versions of columns from the new schema
psql -c 'ALTER TABLE cvdnet_consolidated_deidentified.subjects DROP COLUMN subject_identifier, DROP COLUMN date_of_birth, DROP COLUMN date_of_death' CVD-Net
psql -c 'ALTER TABLE cvdnet_consolidated_deidentified.measurements DROP COLUMN value' CVD-Net

# Left with two schemas:
    # cvdnet_consolidated: the original database (no changes)
    # cvdnet_consolidated_deidentified: modified database with non-de-identified columns removed

# Prepares a dump of cvdnet_consolidated_deidentified for export to Insights (Tier 2) TRE
date_stamp=$(date +"%Y-%m-%d_%H%M%S")
pg_dump --dbname=CVD-Net --schema=cvdnet_consolidated_deidentified --file=deidentified_database_dump_"$date_stamp".sql

# Then migrate the deidentified dump file to the Insights TRE
# Load the database onto the database server (using psql -f [FILENAME] [DATBASE_NAME])
