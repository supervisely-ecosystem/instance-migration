# Instance Migration Scripts Manual

This manual provides instructions on how to use the Stage 1 and Stage 2 scripts from this repository.
Ensure you have the necessary environment variables set before running the scripts.

## Prerequisites

1. Python 3.x installed on your system.
2. Required Python packages installed. You can install them using:
   ```sh
   pip install -r requirements.txt
   ```
3. Environment variables set in your system or you can manually set them in `local.env`.

## Environment Variables

- `LOG_LEVEL`: Logging level (e.g., DEBUG, INFO, WARNING).
- `DATA_PATH`: Path to the directory with the data on the local machine where the instance is running.
- `ENDPOINT_PATH`: "fs endpoint" must be mounted to the same machine where the instance is running. Look for it in Instance Settings -> Cloud Credentials.
- `ROOT_DIR_NAME`: Root directory name, where all the data will be stored during the Stage 1.
- `BUCKET_NAME`: Bucket name. Look for it in Instance Settings -> Cloud Credentials.
- `SEMAPHORE_SIZE`: Number of concurrent tasks to run in parallel. It will help to communicate with the Supervisely API properly.
- `MAX_RETRY`: Number of retries for failed items.

## Stage 1: Data Copying

⚠️ Currently works only with Image and Video project types.

The Stage 1 script collects data for items in projects for all teams and copies it to the destination endpoint path within a human-readable structure.

```python
{ENDPOINT_PATH}/{ROOT_DIR_NAME}/{team_id}-{team_name}/{workspace_id}-{workspace_name}/{project_id}-{project_name}/{dataset_id}-{dataset_name}/{item_id}-{item_name}
```

For example: `/home/user/NAS/migrated/1-Main/1-Test/222-Test_project/333-ds01/12345-lemons.jpg`

### Usage

```sh
python3 stage_1_copy.py
```

### Description

- Connects to the Supervisely API.
- Extracts the necessary data for items in projects for all teams.
- Creates an items map for projects and stores it in JSON format.
- Copies items to the new storage in a human-readable structure.

## Stage 2: Changing the Data Source

The Stage 2 script changes the data source to the data that was migrated in Stage 1.

### Usage

```sh
python3 stage_2_change_source.py
```

### Description

- Reads the data from the local JSON file created by Stage 1.
- Changes the data source for all items in projects with the API call.

## Logging

Logs are generated based on the `LOG_LEVEL`. Ensure the log level is appropriately set for your needs.

## Example

1. Set the environment variables in `local.env`:

2. Run Stage 1 script:

   ```sh
   python3 stage_1_copy.py
   ```

   ☝️ If you get a "Failed items" message in the results, run `stage_1_copy.py` again to try to fix them automatically.

3. Run Stage 2 script:
   ```sh
   python3 stage_2_change_source.py
   ```
