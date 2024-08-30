from pathlib import Path

PARENT_DIR = Path(__file__).parent.resolve().parent
DATA_DIR = PARENT_DIR/'data'
RAW_DATA_DIR = PARENT_DIR/'data'/'raw'
TRANSFORMED_DATA_DIR = PARENT_DIR/'data'/'transformed'

# Create the directories if they don't exist
DATA_DIR.mkdir(exist_ok=True, parents=True)
RAW_DATA_DIR.mkdir(exist_ok=True, parents=True)
TRANSFORMED_DATA_DIR.mkdir(exist_ok=True, parents=True)