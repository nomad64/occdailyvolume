# OCC Daily Volume

## About
This utility returns stats about daily volume statistics from The Options Clearing Corporation. It uses the publicly-available data from https://marketdata.theocc.com/.

## How To Use
Use the below steps as a guide to using this utility.
1. Download the code or clone the repo.
2. Use `pip` to install the necessary packages
    ```
    pip install -Ur requirements.txt
    ```
3. View the help documentation to ensure things run OK
    ```
    python volume-top-n.py --help
    ```
4. Run the utility, updating the database first with current data
    ```
    python volume-top-n.py --update
    ```
