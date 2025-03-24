import os
import logging
import pandas as pd
from typing import Optional, List, Dict, Any
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def detect_delimiter(file_path: str, sample_lines: int = 5) -> str:
    """
    Attempt to detect the delimiter in a file by analyzing a sample of lines.
    """
    common_delimiters = [',', '\t', '|', ';', ' ']
    delimiter_counts = {d: 0 for d in common_delimiters}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= sample_lines:
                    break
                for delim in common_delimiters:
                    delimiter_counts[delim] += line.count(delim)
    except Exception as e:
        logger.warning(f"Error analyzing delimiters: {e}")
        return ","  # Default to comma if detection fails
    
    # Return the delimiter with the highest count
    return max(delimiter_counts.items(), key=lambda x: x[1])[0] or ","

def convert_data(
    input_path: str, 
    output_path: str, 
    delimiter: Optional[str] = None, 
    has_headers: bool = False,
    output_format: str = "csv"
) -> None:
    """
    Convert a data file to CSV or Excel format without creating column names.
    """
    try:
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Auto-detect delimiter if not specified
        if delimiter is None:
            delimiter = detect_delimiter(str(input_path))
            logger.info(f"Detected delimiter: '{delimiter}'")
        
        # Read file with the appropriate parameters
        if has_headers:
            # If file has headers, read normally
            df = pd.read_csv(str(input_path), delimiter=delimiter, encoding='utf-8')
        else:
            # If no headers, read without creating header names
            df = pd.read_csv(str(input_path), delimiter=delimiter, header=None, encoding='utf-8')
        
        # Create output directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save the output file
        if output_format.lower() == 'excel':
            df.to_excel(output_path, index=False, header=has_headers, engine='openpyxl')
        else:
            df.to_csv(output_path, index=False, header=has_headers)
        
        logger.info(f"Successfully converted '{input_path}' to '{output_path}'")
        logger.info(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
        
    except Exception as e:
        logger.error(f"Error converting {input_path}: {str(e)}")
        raise

def batch_convert_data(
    input_folder: str = "data", 
    output_folder: str = "output",
    file_extensions: List[str] = [".dat", ".log", ".txt"],
    output_format: str = "csv",
    has_headers: bool = False,
    custom_delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convert all supported files in the input folder to CSV or Excel.
    """
    # Ensure output folder exists
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    stats = {
        "total_files": 0,
        "successful": 0,
        "failed": 0,
        "failed_files": []
    }
    
    # Find all files with matching extensions
    input_path = Path(input_folder)
    all_files = []
    for ext in file_extensions:
        all_files.extend(list(input_path.glob(f"*{ext}")))
    
    stats["total_files"] = len(all_files)
    
    # Process each file
    for i, file_path in enumerate(all_files):
        try:
            logger.info(f"Processing file {i+1}/{len(all_files)}: {file_path.name}")
            
            # Determine output path based on format
            output_extension = ".xlsx" if output_format.lower() == "excel" else ".csv"
            output_path = Path(output_folder) / f"{file_path.stem}{output_extension}"
            
            convert_data(
                str(file_path),
                str(output_path),
                delimiter=custom_delimiter,
                has_headers=has_headers,
                output_format=output_format
            )
            stats["successful"] += 1
            
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {e}")
            stats["failed"] += 1
            stats["failed_files"].append(str(file_path))
    
    # Display summary
    logger.info(f"Conversion complete: {stats['successful']} successful, "
                f"{stats['failed']} failed")
    
    return stats

if __name__ == "__main__":
    # Entry point
    batch_convert_data()