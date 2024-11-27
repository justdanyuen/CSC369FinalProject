import csv

# File paths
input_file = "population_density_raw.csv"  # Replace with your input file name
output_file = "population_density_normalized.csv"  # Replace with your output file name

# Initialize min and max values for population density
stats = {
    "population_density_per_sqkm": {"min": float("inf"), "max": float("-inf")}
}

# Step 1: Calculate min and max values
with open(input_file, 'r') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        # Update min and max for population_density_per_sqkm
        density = float(row["population_density_per_sqkm"])
        stats["population_density_per_sqkm"]["min"] = min(stats["population_density_per_sqkm"]["min"], density)
        stats["population_density_per_sqkm"]["max"] = max(stats["population_density_per_sqkm"]["max"], density)

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
        # Normalize population_density_per_sqkm
        row["population_density_per_sqkm"] = normalize(
            float(row["population_density_per_sqkm"]), 
            stats["population_density_per_sqkm"]["min"], 
            stats["population_density_per_sqkm"]["max"]
        )
        writer.writerow(row)

print(f"Normalized data has been written to {output_file}.")
