import os
import glob
import shutil
import subprocess
from datetime import datetime, timedelta

# =========================
# CONFIGURATION
# =========================

OUTPUT_DIR = "generated_data"

STREAM_DIR = os.path.join(
    OUTPUT_DIR,
    "stream_batches"
)

CUSTOMERS_FILE = os.path.join(
    OUTPUT_DIR,
    "customers.csv"
)

HISTORICAL_FILE = os.path.join(
    OUTPUT_DIR,
    "historical_transactions.csv"
)

STATE_FILE = "stream_state.txt"

TEMP_OUTPUT_DIR = "temp_output"

CUSTOMER_COUNT = 2000

STREAM_START_DATE = "07-01-2024"

MAX_STREAM_TRANSACTIONS = 100

# =========================
# CREATE DIRECTORIES
# =========================

os.makedirs(OUTPUT_DIR, exist_ok=True)

os.makedirs(STREAM_DIR, exist_ok=True)

# =========================
# DETERMINE MODE
# =========================

historical_exists = os.path.exists(
    HISTORICAL_FILE
)

# =========================
# HISTORICAL MODE
# =========================

if not historical_exists:

    MODE = "historical"

    START_DATE = "01-01-2024"

    END_DATE = "06-30-2024"

    FINAL_OUTPUT_FILE = HISTORICAL_FILE

# =========================
# STREAM MODE
# =========================

else:

    MODE = "stream"

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    FINAL_OUTPUT_FILE = os.path.join(
        STREAM_DIR,
        f"stream_batch_{timestamp}.csv"
    )

    # =========================
    # LOAD STREAM STATE
    # =========================

    if not os.path.exists(STATE_FILE):

        current_time = datetime.strptime(
            STREAM_START_DATE,
            "%m-%d-%Y"
        )

    else:

        with open(STATE_FILE, "r") as f:

            current_time = datetime.strptime(
                f.read().strip(),
                "%Y-%m-%d %H:%M:%S"
            )

    # Move stream window forward
    next_time = current_time + timedelta(
        minutes=5
    )

    START_DATE = current_time.strftime(
        "%m-%d-%Y"
    )

    END_DATE = current_time.strftime(
        "%m-%d-%Y"
    )

print(f"\nRunning in {MODE.upper()} mode...\n")

# =========================
# CLEAN TEMP DIRECTORY
# =========================

if os.path.exists(TEMP_OUTPUT_DIR):

    print("Removing old temp directory...\n")

    shutil.rmtree(TEMP_OUTPUT_DIR)

os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)

# =========================
# GENERATE DATA
# =========================

print("Generating Sparkov transaction data...\n")

command = [
    "/home/ubuntu/Sparkov_Data_Generation/venv/bin/python",
    "datagen.py",
    "-n",
    str(CUSTOMER_COUNT),
    "-o",
    TEMP_OUTPUT_DIR,
    START_DATE,
    END_DATE
]

# =========================
# REUSE CUSTOMERS
# =========================

if MODE == "stream" and os.path.exists(CUSTOMERS_FILE):

    print("Reusing existing customers file...\n")

    command.extend([
        "-c",
        CUSTOMERS_FILE
    ])

# =========================
# RUN GENERATOR
# =========================

subprocess.run(command)

# =========================
# SAVE CUSTOMERS
# =========================

temp_customers = os.path.join(
    TEMP_OUTPUT_DIR,
    "customers.csv"
)

if MODE == "historical":

    if os.path.exists(temp_customers):

        shutil.copy(
            temp_customers,
            CUSTOMERS_FILE
        )

        print("Customers file saved.\n")

# =========================
# MERGE CSV FILES
# =========================

print("Merging transaction CSV files...\n")

csv_files = glob.glob(
    f"{TEMP_OUTPUT_DIR}/*.csv"
)

transaction_files = []

for file in csv_files:

    if "customers.csv" not in file:

        transaction_files.append(file)

if not transaction_files:

    print("No transaction files found.")

    exit()

# Sort files for consistency
transaction_files.sort()

# =========================
# CREATE MERGED FILE
# =========================

with open(FINAL_OUTPUT_FILE, "w") as outfile:

    # Write header once
    with open(transaction_files[0], "r") as first_file:

        header = first_file.readline()

        outfile.write(header)

    # Merge all rows
    for file in transaction_files:

        with open(file, "r") as infile:

            next(infile)

            for line in infile:

                # Skip broken rows
                if line.count("|") < 25:

                    continue

                outfile.write(line)

print("Merge complete.\n")

# =========================
# LIMIT STREAM SIZE
# =========================

if MODE == "stream":

    print("Limiting stream batch size...\n")

    with open(FINAL_OUTPUT_FILE, "r") as f:

        lines = f.readlines()

    header = lines[0]

    data = lines[1:]

    # Limit transaction count
    data = data[:MAX_STREAM_TRANSACTIONS]

    with open(FINAL_OUTPUT_FILE, "w") as f:

        f.write(header)

        f.writelines(data)

    print(
        f"Stream batch limited to "
        f"{len(data)} transactions.\n"
    )

# =========================
# CLEAN TEMP FILES
# =========================

print("Cleaning temporary files...\n")

if os.path.exists(TEMP_OUTPUT_DIR):

    shutil.rmtree(TEMP_OUTPUT_DIR)

print("Cleanup complete.\n")

# =========================
# SAVE STREAM STATE
# =========================

if MODE == "stream":

    with open(STATE_FILE, "w") as f:

        f.write(
            next_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    print(
        f"Next stream state: "
        f"{next_time}"
    )

# =========================
# FINAL OUTPUT
# =========================

print("\nDone.\n")

print("Generated files:")

if os.path.exists(CUSTOMERS_FILE):

    print(f"- {CUSTOMERS_FILE}")

print(f"- {FINAL_OUTPUT_FILE}")
