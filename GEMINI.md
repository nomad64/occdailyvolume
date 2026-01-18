## Project Overview

`occdailyvolume` is a Python-based CLI tool and data pipeline that aggregates daily financial volume data from the [Options Clearing Corporation (OCC)](https://theocc.com). It fetches historical and ongoing data, stores it in a local SQLite database, and allows querying for high-volume days.

## Technical Architecture

The project is structured as a Python package with a main entry point and a shared library module.

### Core Components

-   **`volume-top-n.py`**: The CLI entry point. Handles argument parsing, configuration loading, and orchestration of the update/query workflows.
-   **`common/`**: A package containing the core logic:
    -   `occ.py`: Handles HTTP requests to OCC, parses the raw CSV responses, and cleans the data (removing summary rows like "YTD", "Avg").
    -   `updater.py`: Implements the synchronization logic. It creates a local `volume-top-n.sqlite` database and backfills data month-by-month from the current date back to January 2008. It also handles forward-filling if the database is outdated.
    -   `sqlite.py`: Manages SQLite connections and DataFrame I/O.
    -   `dataframe.py`: Utilities for formatting and printing Pandas DataFrames (uses `tabulate`).
    -   `yaml.py`: Configuration loader.

### Dependencies

-   **Runtime**: `python-3`
-   **Data Analysis**: `pandas`
-   **HTTP Client**: `requests`
-   **Configuration**: `pyyaml`
-   **Utilities**: `python-dateutil`, `argparse`, `tabulate`

## Configuration

Configuration is managed via `occ-daily-volume/volume-top-n.yaml`.

```yaml
database:
  sqlite:
    db_filepath: data/volume-top-n.sqlite  # Path to the SQLite DB
    db_table: volHist                      # Table name for volume data

occweb:
  daily_volume_url: https://marketdata.theocc.com/daily-volume-statistics
  daily_volume_format: csv
```

## Data Pipeline

1.  **Ingestion**: The tool requests monthly volume reports from OCC.
2.  **Cleaning**: Raw CSV data is split (contracts vs. futures), and summary lines (e.g., "Oct Total", "YTD Total") are programmatically removed to ensure only daily records remain.
3.  **Storage**: Cleaned data is appended to the SQLite `volHist` table.
    -   **Schema**: `Date` (Index), `Equity`, `Index/Others`, `Debt`, `Futures`, `OCC Total`.
4.  **Querying**: The tool reads the database into a Pandas DataFrame to perform aggregations and sorting (e.g., finding top N volume days).

## Usage

### Local CLI

1.  **Install dependencies**:
    ```bash
    pip install -r occ-daily-volume/requirements.txt
    ```
2.  **Run the tool**:
    ```bash
    # Update database and show top 10 days
    python occ-daily-volume/volume-top-n.py --update --number 10
    ```

### Docker

The project includes a `Dockerfile` for containerized execution.

```bash
# Build
docker build -t occ-daily-volume .

# Run (mounting a local volume for persistence)
docker run -v $(pwd)/occ-daily-volume/data:/app/data occ-daily-volume
```

## Development

### Testing

Tests are located in `occ-daily-volume/tests/`.

Run tests using `pytest`:
```bash
pytest occ-daily-volume/tests
```

### Conventions

-   **Style**: PEP 8.
-   **Logging**: Standard `logging` library. Default level is `WARNING`, adjustable via `--log-level`.
-   **Paths**: All paths in config should be relative to the script execution or absolute.