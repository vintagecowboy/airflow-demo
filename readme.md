#Overview
This airflow dag: 
- populates a BigQuery dataset with public EU referendum data, from a CSV file stored in GCS (Google Cloud Storage).

- checks for the existence of a geo dataset; this can be loaded into BQ from the admin_areas.avro file in the bigquery_sql subdirectory.

- populates a master reporting table, combining the referendum data and geo data by executing the geo.sql script in the bigquery_sql subdirectory.
