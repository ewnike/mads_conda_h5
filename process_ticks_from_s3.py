import argparse
from io import StringIO

import boto3
import numpy as np
import pandas as pd
import tables

from shared_types import tick_type


class H5TickType(tables.IsDescription):
    """
    Description of the HDF5 table structure for tick data.
    """

    symbol = tables.StringCol(10)  # Adjusted length to accommodate longer symbols
    date = tables.Int64Col()
    time = tables.Int64Col()
    last_p = tables.Float64Col()
    last_v = tables.Int64Col()


def read_csv_from_s3(bucket_name, s3_file_key, region_name="us-east-2"):
    """Read CSV file directly from S3."""
    s3 = boto3.client("s3", region_name=region_name)
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_file_key)
        csv_string = response["Body"].read().decode("utf-8")
        data = pd.read_csv(StringIO(csv_string))
        print(f"Read {s3_file_key} from {bucket_name} successfully.")
        return data
    except Exception as e:
        print(f"Error reading {s3_file_key} from {bucket_name}: {e}")
        return None


def process_csv_to_hdf5(data, h5file, h5path, sym):
    """Process data and insert into PyTables."""
    try:
        h5table = h5file.create_table(h5path, sym, H5TickType, f"{sym} table")
        row = h5table.row
        for row_data in data.itertuples(index=False):
            date = np.datetime64(row_data.date + " " + row_data.time)
            row["symbol"] = sym
            row["date"] = date.astype("M8[D]").astype(np.int64)
            row["time"] = (date - date.astype("M8[D]")).astype(np.int64)
            row["last_p"] = row_data.last_p
            row["last_v"] = row_data.last_v
            row.append()
        h5file.flush()
        print(f"Processed and inserted data for {sym} into HDF5 file.")
    except Exception as e:
        print(f"Error processing data for {sym}: {e}")


def make_h5(sym, h5_filename=None):
    """
    Write data to table.
    """
    if h5_filename is None:
        h5_filename = f"tick_data_{sym}.h5"
    return tables.open_file(h5_filename, mode="w", title=f"{sym} Tick Data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process tick data from S3.")
    parser.add_argument(
        "--bucket", type=str, required=True, help="The name of the S3 bucket"
    )
    parser.add_argument(
        "--region",
        type=str,
        default="us-east-2",
        help="The AWS region of the S3 bucket",
    )
    args = parser.parse_args()

    bucket_name = args.bucket
    region_name = args.region

    s3_file_keys = {
        "Wheat": "WC.csv",
        "Corn": "CN.csv",
        "Soy": "SY.csv",
        "SoyMeal": "SM.csv",
        "BeanOil": "BO.csv",
    }

    for sym, s3_file_key in s3_file_keys.items():
        print(f"Attempting to read {s3_file_key} from bucket {bucket_name}")
        data = read_csv_from_s3(bucket_name, s3_file_key, region_name)

        if data is not None:
            h5_filename = f"tick_data_{sym}.h5"
            h5file = make_h5(sym, h5_filename)

            process_csv_to_hdf5(data, h5file, "/", sym)

            h5file.close()
            print(f"Closed HDF5 file {h5_filename}.")
        else:
            print(
                f"Skipping processing for {sym} as the data was not read successfully."
            )
