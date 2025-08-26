from src.collector import fetch_and_save_crypto_data, fetch_crypto_data_endpoint
from src.parquet_storage import (
    read_crypto_data_from_parquet,
    upload_parquet_to_r2,
    batch_upload_to_r2,
)


def main():
    print("Testing crypto historical data fetching and parquet storage...")

    # Example 1: Fetch and save data to parquet
    print("\n1. Fetching and saving BTCUSD data for 2024-01-01:")
    result1 = fetch_and_save_crypto_data("BTCUSD", specific_date="2024-01-01")
    print(
        "API Result keys:",
        list(result1["api_result"].keys()) if result1["api_result"] else None,
    )
    print("Storage Result:", result1["storage_result"])

    # Example 2: Read data from parquet
    print("\n2. Reading BTCUSD data from parquet file:")
    parquet_data = read_crypto_data_from_parquet("BTCUSD", "2024-01-01")
    print(parquet_data)

    # Example 3: Upload to R2 (if file exists)
    if result1["storage_result"].get("success"):
        print("\n3. Uploading parquet file to Cloudflare R2:")
        file_path = result1["storage_result"]["file_path"]
        upload_result = upload_parquet_to_r2(file_path)
        print(upload_result)

    # Example 4: Batch upload demonstration
    print("\n4. Testing batch upload (files older than 0 days):")
    batch_result = batch_upload_to_r2(days_old=0)
    print(batch_result)

    # Example 5: Test validation
    print("\n5. Testing invalid date format:")
    result3 = fetch_crypto_data_endpoint("BTCUSD", specific_date="01-01-2024")
    print(result3)


if __name__ == "__main__":
    main()
