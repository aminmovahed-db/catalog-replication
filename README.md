# catalog-replication

This repository contains a series of Databricks notebooks designed to manage a data catalog replication within a metastore (the current scope is only for external tables in unity catalog). The workflow includes preparation, backup, destruction, recreation, and comparison of the data catalog, alongside managing Hive Metastore views.

## Overview

The notebooks in this repository are part of a workflow to backup, and proper management of a data catalog. This is crucial for environments where data schemas and structures are critical and need regular backups and the ability to restore to a previous state or compare changes over time. The backup method is based on the information captured in information schema of the catalog.

## Notebooks

- `00_prepare_catalog.py`: Initializes a catalog for test, setting up schemas, tables, or initial data loads as necessary (optional for testing purposes).
- `01_backup_catalog.py`: Backs up the catalog, including schemas and data, to a secure location for disaster recovery or archival purposes.
- `02_destroy_catalog.py`: Cleans up or removes the catalog, which might be part of a teardown process in a development environment or to refresh the catalog's state.
- `03_recreate_catalog.py`: Recreates the catalog from scratch after destruction, useful in testing environments or when a clean state is required.
- `04_compare_catalog_with_backup.py`: Compares the current state of the catalog with its backup to ensure completeness and accuracy or to identify changes/drifts.

# Extra
- `HMS_views_backup.py`: Manages the backup of Hive Metastore - related to the Hive Metastore.

## Getting Started

To get started with this workflow:

1. Ensure you have the necessary environment set up, including access to a data catalog and any required storage locations for backups.
2. Clone this repository to your local machine or development environment.
3. Run the notebooks in sequence, starting with `01_prepare_catalog.py` to backup the catalog.

## Contact

For any questions or contributions, please contact amin.movahed@databricks.com
