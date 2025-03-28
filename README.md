# Data Conversion Tool

A simple Python utility for converting data files to CSV/Excel format.

## Features

- Automatic delimiter detection
- Batch processing of multiple files
- Support for CSV and Excel output
- Works with or without headers
- Simple logging and error handling

## Usage

```python
# Convert a single file
from convert_data2 import convert_data
convert_data("input.txt", "output.csv")

# Batch convert files
from convert_data2 import batch_convert_data
batch_convert_data(
    input_folder="data",
    output_folder="output",
    file_extensions=[".txt", ".dat", ".log"],
    has_headers=False
)