"""
Test script for bridge processor development.
Downloads mock data from S3 and tests the bridge masking functionality on a depth library that contains rasters that intersect bridges and then runs the extent library generation process to make sure extent generation still works.
"""

import subprocess
import sys
from pathlib import Path

# Add project root to path for src directory imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

from src.process.bridge_processor import process_bridges
from src.process.extent_library import create_extent_lib
from src.setup.collection_data import CollectionData


def ensure_mock_data(mock_data_dir: Path, s3_collection: str):
    """Download mock data from S3, always overwriting existing data since local mock data could have been changed by one of the processes being tested"""
    mock_data_dir.mkdir(parents=True, exist_ok=True)

    library_dir = mock_data_dir / "library"
    submodels_dir = mock_data_dir / "submodels"

    print("Downloading depth library from S3...")
    subprocess.run(
        ["aws", "s3", "cp", "--recursive", f"{s3_collection}/library/", str(library_dir)],
        check=True,
    )

    print("Downloading submodels gpkg files from S3...")
    subprocess.run(
        [
            "aws",
            "s3",
            "cp",
            "--recursive",
            f"{s3_collection}/submodels/",
            str(submodels_dir),
            "--exclude",
            "*",
            "--include",
            "*.gpkg",
        ],
        check=True,
    )

    print("Downloading submodels Terrain directories from S3...")
    subprocess.run(
        [
            "aws",
            "s3",
            "cp",
            "--recursive",
            f"{s3_collection}/submodels/",
            str(submodels_dir),
            "--exclude",
            "*",
            "--include",
            "*/Terrain/*",
        ],
        check=True,
    )
    return library_dir, submodels_dir


def create_test_config(mock_data_dir: Path):
    """Create a test config.yaml with local mock data paths."""
    config_path = Path(__file__).parent.parent / "src" / "config.yaml"
    test_config_path = Path(__file__).parent / "test_config.yaml"

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Override paths for testing
    config["paths"]["COLLECTIONS_ROOT_DIR"] = str(mock_data_dir.parent)

    # use Linux system GDAL instead of Windows paths
    config["flows2fim"]["GDAL_BINS_PATH"] = ""
    config["flows2fim"]["GDAL_SCRIPTS_PATH"] = ""

    # Write test config
    with open(test_config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    return test_config_path


def test_bridge_processor():
    """Main test function for bridge processor development."""

    mock_data_dir = Path(__file__).parent.parent / "data" / "mock"
    s3_collection = "s3://fimc-data/ripple/fim_100_domain/collections/mip_18060013"

    print("Setting up mock data and CollectionData...")
    ensure_mock_data(mock_data_dir, s3_collection)
    test_config_path = create_test_config(mock_data_dir)
    # The "mock" collection ID combined with COLLECTIONS_ROOT_DIR=data/
    # will result in root_dir=data/mock which contains our mock data
    collection = CollectionData("mock", config_file=str(test_config_path))

    print("\nRunning bridge processor...")
    result = process_bridges(collection, print_progress=True)

    print(f"\nFiles with depth values modified: {len(result['modified'])}\n")
    if result["modified"]:
        for f in result["modified"]:
            print(f"  {f}")
    else:
        print("  (none)")

    print(f"\nFiles with bridge intersections: {len(result['with_bridges'])}")
    print(f"\nFiles without bridge intersections: {len(result['without_bridges'])}")

    print("\nRunning extent library creation...")
    create_extent_lib(collection, print_progress=True)

    print("\nAll processing steps complete!\n")


if __name__ == "__main__":
    test_bridge_processor()
