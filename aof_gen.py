# Define the output file path
output_file = "output.txt"

# Open the file for writing
with open(output_file, "w") as file:
    
    for i in range(1000000):
        # Determine the key and value
        key = "string" + str(i)
        value = str(i)

        # Write the pattern to the file
        file.write("*3\n$3\nSET\n")
        file.write(f"${len(key)}\n{key}\n")
        file.write(f"${len(value)}\n{value}\n")

print(f"Generated {output_file} with 1000000 entries.")