import glob
import pandas as pd
from clickhouse_driver import Client
from pathlib import Path
import platform
import os
from dotenv import load_dotenv

load_dotenv()

def get_clickhouse_client():
    return Client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        user=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD']
    )

DB = "vkusvill_db"
BATCH_SIZE = 300

def safe_float(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value == "Ждёт оценку":
        return None

    try:
        return float(value.replace(",", "."))
    except:
        return None

client = get_clickhouse_client()
DATA_DIR = Path("parsed_items")

files = list(DATA_DIR.glob("*.csv"))

for file in files:
    print(f"Loading {file}")

    df = pd.read_csv(file)
    
    df["price"] = (
        pd.to_numeric(df["price"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    for col in [
        "rating",
        "calories",
        "proteins",
        "fats",
        "carbs"
    ]:
        df[col] = df[col].apply(safe_float)

    df["parsed_at"] = pd.to_datetime(df["parsed_at"])

    print("\nNULL statistics:")
    for col in df.columns:
        cnt = df[col].isna().sum()
        if cnt > 0:
            print(f"{col}: {cnt}")

    df = df.dropna(
        subset=[
            "title",
            "category",
            "subcategory",
            "url"
        ]
    )
    
    records = []
    for _, row in df.iterrows():
        records.append((
            row["title"],
            row["category"],
            row["subcategory"],
            int(row["price"]),
            row["price_unit"],
            None if pd.isna(row["weight"]) else row["weight"],
            row["rating"],
            None if pd.isna(row["description"]) else row["description"],
            row["calories"],
            row["proteins"],
            row["fats"],
            row["carbs"],
            None if pd.isna(row["shelf_life"]) else row["shelf_life"],
            "" if pd.isna(row["brand"]) else row["brand"],
            None if pd.isna(row["storage_conditions"]) else row["storage_conditions"],
            None if pd.isna(row["manufacturer"]) else row["manufacturer"],
            None if pd.isna(row["country"]) else row["country"],
            None if pd.isna(row["composition"]) else row["composition"],
            None if pd.isna(row["labels"]) else row["labels"],
            row["url"],
            row["parsed_at"].to_pydatetime()
        ))

    total_records = len(records)
    inserted = 0
    
    for i in range(0, total_records, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        try:
            client.execute(
                f"INSERT INTO {DB}.products VALUES",
                batch
            )
            inserted += len(batch)
            print(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} rows (total: {inserted}/{total_records})")
            
        except Exception as e:
            print(f"\nERROR in batch #{i//BATCH_SIZE + 1}")
            print(f"Batch size: {len(batch)}")
            print(f"First record in batch: {batch[0] if batch else 'empty'}")
            print(f"Error: {e}")
            raise

    print(f"\n✅ File {file.name} complete. Inserted {inserted} rows\n")