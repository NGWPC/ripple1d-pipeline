"""
Test script for bridge processor development.
Downloads mock data from S3 and tests the bridge masking functionality.
"""

import subprocess
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

from src.setup.collection_data import CollectionData

# === Configuration ===
MOCK_DATA_DIR = Path(__file__).parent.parent / "data" / "mock"
S3_PROFILE = "fimbucket"
S3_COLLECTION = "s3://fimc-data/ripple/fim_100_domain/collections/mip_18060013"
S3_BRIDGE_INDEX = "s3://fimc-data/benchmark/stac-bench-cat/assets/bridge_index.parquet"


def ensure_mock_data():
    """Download mock data from S3 if not already present."""
    MOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    library_dir = MOCK_DATA_DIR / "library"
    submodels_dir = MOCK_DATA_DIR / "submodels"

    # Download library if not present
    if not library_dir.exists() or not any(library_dir.iterdir()):
        print("Downloading depth library from S3...")
        subprocess.run(
            ["aws", "s3", "sync", f"{S3_COLLECTION}/library/", str(library_dir), "--profile", S3_PROFILE], check=True
        )

    # Download submodels (gpkg and Terrain) if not present
    if not submodels_dir.exists() or not any(submodels_dir.iterdir()):
        print("Downloading submodels (gpkg files) from S3...")
        subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                f"{S3_COLLECTION}/submodels/",
                str(submodels_dir),
                "--profile",
                S3_PROFILE,
                "--exclude",
                "*",
                "--include",
                "*.gpkg",
            ],
            check=True,
        )

        print("Downloading submodels (Terrain directories) from S3...")
        subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                f"{S3_COLLECTION}/submodels/",
                str(submodels_dir),
                "--profile",
                S3_PROFILE,
                "--exclude",
                "*",
                "--include",
                "*/Terrain/*",
            ],
            check=True,
        )

    # Bridge index is read directly from S3 - no download needed
    print(f"Mock data ready at: {MOCK_DATA_DIR}")
    print(f"Bridge index will be read from S3: {S3_BRIDGE_INDEX}")
    return library_dir, submodels_dir


def create_test_config():
    """Create a test config.yaml with local mock data paths."""
    config_path = Path(__file__).parent.parent / "src" / "config.yaml"
    test_config_path = Path(__file__).parent / "test_config.yaml"

    # Read original config
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Override paths for testing
    # Library/submodels are local, bridge index stays on S3
    config["paths"]["COLLECTIONS_ROOT_DIR"] = str(MOCK_DATA_DIR.parent)
    # Keep bridge index as S3 path - pandas can read directly with storage_options
    config["paths"]["BRIDGE_TILE_INDEX_PATH"] = S3_BRIDGE_INDEX

    # Override GDAL paths for Linux (use system GDAL instead of Windows paths)
    config["flows2fim"]["GDAL_BINS_PATH"] = ""
    config["flows2fim"]["GDAL_SCRIPTS_PATH"] = ""

    # Write test config
    with open(test_config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    return test_config_path


def create_mock_collection_data():
    """
    Create a CollectionData object configured for mock data.

    This bypasses the normal CollectionData initialization to point
    directly at our mock data directories.
    """
    # Ensure mock data is downloaded (library + submodels only)
    ensure_mock_data()

    # Create test config
    test_config_path = create_test_config()

    # Create CollectionData with mock collection ID
    # The "mock" collection ID combined with COLLECTIONS_ROOT_DIR=data/
    # will result in root_dir=data/mock which contains our mock data
    collection = CollectionData("mock", config_file=str(test_config_path))

    return collection


def test_bridge_processor():
    """Main test function for bridge processor development."""
    from src.process.bridge_processor import process_bridges
    from src.process.extent_library import create_extent_lib

    print("Setting up mock data and CollectionData...")
    collection = create_mock_collection_data()

    print(f"Library dir: {collection.library_dir}")
    print(f"Submodels dir: {collection.submodels_dir}")
    print(f"Extent library dir: {collection.extent_library_dir}")
    print(f"Bridge index (S3): {collection.bridge_tile_index_path}")

    print("\n" + "=" * 50)
    print("Step 1: Running bridge processor...")
    print("=" * 50)
    result = process_bridges(collection, print_progress=True)

    # Display files that had actual depth values modified
    print("\n" + "-" * 50)
    print(f"Files with depth values modified: {len(result['modified'])}")
    print("-" * 50)
    if result["modified"]:
        for f in result["modified"]:
            print(f"  {f}")
    else:
        print("  (none)")

    print(f"\nFiles with bridge intersections: {len(result['with_bridges'])}")
    print(f"Files without bridge intersections: {len(result['without_bridges'])}")

    print("\n" + "=" * 50)
    print("Step 2: Running extent library creation...")
    print("=" * 50)
    create_extent_lib(collection, print_progress=True)

    print("\n" + "=" * 50)
    print("All processing steps complete!")
    print("=" * 50)


if __name__ == "__main__":
    test_bridge_processor()
