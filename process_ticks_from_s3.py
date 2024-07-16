import argparse
import os

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


def download_from_s3(
    bucket_name, s3_file_key, local_file_path, region_name="us-east-2"
):
    """Download file from S3."""
    s3 = boto3.client("s3", region_name=region_name)
    try:
        s3.download_file(bucket_name, s3_file_key, local_file_path)
        print(f"Downloaded {s3_file_key} from {bucket_name} to {local_file_path}")
    except Exception as e:
        print(f"Error downloading {s3_file_key} from {bucket_name}: {e}")


def process_csv_to_hdf5(csv_file, h5file, h5path, sym):
    """Read CSV file, process lines, and insert into PyTables."""
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} does not exist.")
        return
    try:
        data = pd.read_csv(csv_file)
        print(f"Read CSV file {csv_file} successfully.")

        h5table = h5file.create_table(h5path, sym, H5TickType, f"{sym} table")
        row = h5table.row
        for row_data in data.itertuples(index=False):
            date = np.datetime64(row_data.date + " " + row_data.time)
            row["symbol"] = sym
            row["date"] = date.astype("M8[D]").astype(np.int64)
            row["time"] = date - date.astype("M8[D]").astype(np.int64)
            row["last_p"] = row_data.last_p
            row["last_v"] = row_data.last_v
            row.append()
        h5file.flush()
        print(f"Processed and inserted data from {csv_file} into HDF5 file.")
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")


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
        local_file_path = f"{sym}.csv"
        print(f"Attempting to download {s3_file_key} from bucket {bucket_name}")
        download_from_s3(bucket_name, s3_file_key, local_file_path, region_name)

        if os.path.exists(local_file_path):
            h5_filename = f"tick_data_{sym}.h5"
            h5file = make_h5(sym, h5_filename)

            process_csv_to_hdf5(local_file_path, h5file, "/", sym)
            os.remove(local_file_path)
            print(f"Removed local file {local_file_path} after processing.")

            h5file.close()
            print(f"Closed HDF5 file {h5_filename}.")
        else:
            print(
                f"Skipping processing for {local_file_path} as it was not downloaded."
            )
