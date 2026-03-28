from pathlib import Path
from datetime import datetime
from io import StringIO

import pandas as pd
import psycopg2

DATA_DIR = Path("/.../.../...")
FILES = [DATA_DIR / "taxi_zone_lookup.csv"]

TARGET_COLUMNS = [
    "locationid",
    "borough",
    "zone",
    "service_zone",
    "source_file",
    "load_timestamp",
]

RENAME_MAP = {
    "LocationID": "locationid",
    "Borough": "borough",
    "Zone": "zone",
    "service_zone": "service_zone",
}

conn = psycopg2.connect(
    host="localhost",
    port=5435,
    dbname="nyc_taxi",
    user="admin",
    password="admin",
)

print(f"Found {len(FILES)} files")

for i, file_path in enumerate(FILES, start=1):
    print(f"[{i}/{len(FILES)}] Loading {file_path.name}")

    df = pd.read_csv(file_path)
    df = df.rename(columns=RENAME_MAP)

    df["source_file"] = str(file_path)
    df["load_timestamp"] = datetime.now()

    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[TARGET_COLUMNS]

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep=",", na_rep="")
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            """
            COPY raw.taxi_zone_lookup (
                locationid,
                borough,
                zone,
                service_zone,
                source_file,
                load_timestamp
            )
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER ',',
                NULL ''
            )
            """,
            buffer,
        )
    conn.commit()

    print(f"Inserted {len(df)} rows from {file_path.name}")

conn.close()
print("Done")
