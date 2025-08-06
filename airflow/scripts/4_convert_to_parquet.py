# convert the json into parquet

import json
import pandas as pd
import pathlib

input_dir = pathlib.Path("/opt/airflow/data/raw")  # or path to your JSONs
output_dir = pathlib.Path("/opt/airflow/data/parquet")
output_dir.mkdir(exist_ok=True)   # exist_ok means ignore error if folder already exist

for json_file in input_dir.rglob("*.json"):  # glob -- global pattern matching, r mean recursive
    with open(json_file, "r") as f:
        data = json.load(f)
    
    # Normalize GeoJSON structure
    df = pd.json_normalize(data['features'])

    # Extract coordinates
    df["longitude"] = df["geometry.coordinates"].apply(lambda x: x[0])
    df["latitude"] = df["geometry.coordinates"].apply(lambda x: x[1])
    df["depth_km"] = df["geometry.coordinates"].apply(lambda x: x[2])

    # Rename conflicting columns
    df = df.rename(columns={
        "type": "record_type",                 # root
        "properties.type": "event_type",       # properties
        "geometry.type": "geometry_type"       # geometry
    })

    # Rename for clarity
    df.columns = [col.split(".")[-1] for col in df.columns]

    # hit some datatype error in bigquery so need to make sure columns are correctly casted
    columns_float = ['mag', 'cdi', 'mmi', 'dmin', 'rms', 'gap', 'longitude', 'latitude', 'depth_km']
    columns_int = ['time', 'updated', 'tz', 'felt', 'tsunami', 'sig', 'nst']

    for col in columns_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)

    for col in columns_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(pd.Int64Dtype())
    
    out_file = output_dir / (json_file.stem + ".parquet")  # .stem -- get the filename without extension
    df.to_parquet(out_file, index=False)

    print(f"{out_file} is created successfully.")
