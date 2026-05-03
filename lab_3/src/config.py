from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

LAKEHOUSE_DIR = PROJECT_ROOT / "lakehouse"

BRONZE_PATH = LAKEHOUSE_DIR / "bronze" / "flights"
SILVER_PATH = LAKEHOUSE_DIR / "silver" / "flights"
GOLD_AGG_PATH = LAKEHOUSE_DIR / "gold" / "flight_delay_aggregates"
GOLD_FEATURES_PATH = LAKEHOUSE_DIR / "gold" / "flight_delay_features"