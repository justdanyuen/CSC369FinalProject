import csv

# File paths
input_file = "air_quality_raw.csv"  # Replace with your input file
output_file = "air_quality_normalized.csv"  # Replace with your desired output file

# Initialize min and max values
stats = {
    "co2_ppm": {"min": float("inf"), "max": float("-inf")},
    "pm25_ugm3": {"min": float("inf"), "max": float("-inf")},
    "pm10_ugm3": {"min": float("inf"), "max": float("-inf")},
}

# Step 1: Calculate min and max values
with open(input_file, 'r') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        # Update min and max for co2_ppm
        co2 = float(row["co2_ppm"])
        stats["co2_ppm"]["min"] = min(stats["co2_ppm"]["min"], co2)
        stats["co2_ppm"]["max"] = max(stats["co2_ppm"]["max"], co2)

        # Update min and max for pm25_ugm3
        pm25 = float(row["pm25_ugm3"])
        stats["pm25_ugm3"]["min"] = min(stats["pm25_ugm3"]["min"], pm25)
        stats["pm25_ugm3"]["max"] = max(stats["pm25_ugm3"]["max"], pm25)

        # Update min and max for pm10_ugm3
        pm10 = float(row["pm10_ugm3"])
        stats["pm10_ugm3"]["min"] = min(stats["pm10_ugm3"]["min"], pm10)
        stats["pm10_ugm3"]["max"] = max(stats["pm10_ugm3"]["max"], pm10)

# Print the calculated ranges
print("Calculated Minimum and Maximum Values:")
for key, value in stats.items():
    print(f"{key}: min = {value['min']}, max = {value['max']}")

# Step 2: Normalize and write data to a new CSV file
def normalize(value, min_value, max_value):
    return (value - min_value) / (max_value - min_value) if max_value != min_value else 0

with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames  # Preserve original column names
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)

    # Write header to the output file
    writer.writeheader()

    # Normalize data and write to output
    for row in reader:
        # Normalize each column
        row["co2_ppm"] = normalize(
            float(row["co2_ppm"]), stats["co2_ppm"]["min"], stats["co2_ppm"]["max"]
        )
        row["pm25_ugm3"] = normalize(
            float(row["pm25_ugm3"]), stats["pm25_ugm3"]["min"], stats["pm25_ugm3"]["max"]
        )
        row["pm10_ugm3"] = normalize(
            float(row["pm10_ugm3"]), stats["pm10_ugm3"]["min"], stats["pm10_ugm3"]["max"]
        )
        writer.writerow(row)

print(f"Normalized data has been written to {output_file}.")
