# occ-daily-volume

## Project Overview

This project is a Python-based data pipeline that downloads, processes, and stores daily financial volume data from the Options Clearing Corporation (OCC). It is designed to be run from the command line.

The pipeline fetches monthly data in CSV format from `theocc.com`, cleans it, and stores it in a local SQLite database. The script can backfill historical data and keep the database updated with the latest available monthly data.

The main technologies used are:
- Python 3
- Pandas for data manipulation
- Requests for fetching data from the web
- PyYAML for configuration
- Argparse for command-line argument parsing
- SQLite for the local database

## Building and Running

### Prerequisites

- Python 3
- Pip

### Installation

1.  Install the required Python packages:
    ```bash
    pip install -r occ-daily-volume/requirements.txt
    ```

### Running the script

The main script is `volume-top-n.py`. It can be run from the command line.

```bash
python occ-daily-volume/volume-top-n.py --config occ-daily-volume/volume-top-n.yaml --log-level INFO
```

### Running with Docker

This project includes a `Dockerfile` to build and run the application in a containerized environment.

1.  **Build the Docker image:**
    ```bash
    docker build -t occ-daily-volume .
    ```

2.  **Run the Docker container:**
    ```bash
    docker run -v ./data:/app/data occ-daily-volume
    ```
    This command mounts the local `data` directory into the container, allowing the application to persist the SQLite database on the host machine.

The script uses the `volume-top-n.yaml` file for configuration, including the database path, data URL, and other parameters.

## Development Conventions

- **Configuration**: Project configuration is managed through the `volume-top-n.yaml` file.
- **Database**: The project uses a SQLite database to store the volume data. The database schema is managed by the `volume-top-n.py` script.
- **Logging**: The project uses the standard Python `logging` module. The log level can be set via a command-line argument.
- **Modularity**: The project is organized into a main script (`volume-top-n.py`) and a `common` module for shared functionality (database, logging, YAML parsing).

## AI Assistance

Portions of this project were developed with the assistance of Google's Gemini. Human oversight was used to review and integrate the AI-generated code.
