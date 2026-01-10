import json
import glob     # used to search for and retrieve file paths that match a specific pattern like * or ?
import os

def calculate_total_purchases(file_pattern):    # Sums product quantities for "purchase" across files
    total_quantity = 0     # Starts the total count at zero

    # Loops through every file path discovered by glob
    for file_path in glob.glob(file_pattern):
        with open(file_path, 'r') as f:
            for line in f:
                if not line.strip():    # Skips empty lines to avoid parsing errors
                    continue
                transaction = json.loads(line)     # Converts line from a string to a dictionary
                if transaction.get("transaction_type") == "purchase":
                    for product in transaction.get("products", []):
                        total_quantity += product.get("quantity", 0)

    return total_quantity

# Makes sure the absolute path to the folder where this script is saved
current_folder = os.path.dirname(os.path.abspath(__file__))

# Searches for files in that same folder
search_pattern = os.path.join(current_folder, "transaction_events_*.json")

print(f"Checking folder: {current_folder}")

# Mkaes list of all files that match our search pattern
found_files = glob.glob(search_pattern)

print(f"Files found: {found_files}")

# Check if the file list is empty to prevent errors
if not found_files:
    print("Error: No files found! Check if the JSON files are in the same folder as this script.")
else:
    result = calculate_total_purchases(search_pattern)

    print(f"Success! Total items purchased: {result}")