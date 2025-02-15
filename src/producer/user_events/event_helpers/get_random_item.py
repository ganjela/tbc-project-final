import random
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_item_ids(input_file: str, output_file: str) -> None:
    """
    Extracts numeric ITEM IDs from the input file and writes them to the output file.

    Args:
        input_file (str): Path to the input file containing item data.
        output_file (str): Path to the output file to write extracted item IDs.
    """
    try:
        with open(input_file, 'r', errors='ignore') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                if line.startswith("ITEM"):
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        outfile.write(parts[1] + "\n")
        logging.info(f"Successfully extracted item IDs from {input_file} to {output_file}.")
    except FileNotFoundError:
        logging.error(f"File not found: {input_file}")
    except IOError as e:
        logging.error(f"I/O error({e.errno}): {e.strerror}")

def get_random_item_id(filename: str) -> str:
    """
    Selects a random ITEM ID from the specified file using reservoir sampling.

    Args:
        filename (str): Path to the file containing item IDs.

    Returns:
        str: A randomly selected item ID.

    Raises:
        ValueError: If no ITEM IDs are found in the file.
    """
    selected_item = None
    try:
        with open(filename, 'r') as file:
            for i, line in enumerate(file, start=1):
                if random.randrange(i) == 0:
                    selected_item = line.strip()
        if selected_item is None:
            raise ValueError("No ITEM IDs found in the file.")
    except FileNotFoundError:
        logging.error(f"File not found: {filename}")
        raise
    except IOError as e:
        logging.error(f"I/O error({e.errno}): {e.strerror}")
        raise
    return selected_item