"""
Generate VRT/GTI matrix combinations for bridge masking profiling.

Walks the library directory, selects the best depth TIF per reach (highest
stage, highest flow, skipping corrupt files), and builds bridge-masking-aware
composites comparing two approaches x two granularities x two outer formats
= 8 combinations:

  expression_per_reach_vrt    expression_per_reach_gti
  expression_per_tile_vrt     expression_per_tile_gti
  precomputed_per_reach_vrt   precomputed_per_reach_gti
  precomputed_per_tile_vrt    precomputed_per_tile_gti

Usage:
    pixi run python data/scripts/generate_bridge_vrt_matrix.py --library D:\collections-v2\mip_07050005\library --output D:\collections-v2\mip_07050005\bridge_vrt_matrix

    With S3 path remapping for Linux testing:
    pixi run python data/scripts/generate_bridge_vrt_matrix.py --library D:\collections-v2\mip_07050005\library --output D:\collections-v2\mip_07050005\bridge_vrt_matrix_s3 --s3-library /vsis3/fimc-data/scratch/biplov.bhandari/ripple1d/ripple1d-runs/mip_07050005/library
"""

import argparse
import json
import logging
import math
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SRS = "EPSG:5070"
GTI_LAYER_NAME = "tileindex"
NODATA_SENTINEL = -3.4028235e+38


def run_cmd(cmd: list, description: str) -> subprocess.CompletedProcess:
    result = subprocess.run([str(c) for c in cmd], capture_output=True, text=True)
    if result.returncode != 0:
        logger.debug(f"{description} stdout: {result.stdout}")
        logger.error(f"{description} stderr: {result.stderr}")
        raise RuntimeError(f"{description} failed")
    return result


def bridge_expression(nodata: float) -> str:
    """Build a muparser expression (GDAL 3.12+) for bridge masking.

    B1 = depth, B2 = clearance (bridge_elev - DEM).
    Non-bridge pixels (B2 == 0) return the declared nodata so the VRT/GTI
    compositer treats them as transparent - the depth TIF underneath shows
    through.
    Bridge-above-water pixels return nan, which compositing treats
    as valid data (nan != nodata) so it stays opaque, but renderers like QGIS
    display nan as transparent.
    """
    return f"(B2 == 0) ? {nodata} : ((B1 - B2 >= 0) ? (B1 - B2) : nan)"


@dataclass
class RasterMeta:
    width: int
    height: int
    geotransform: list[float]
    bounds: tuple[float, float, float, float]
    res: tuple[float, float]
    nodata: float


def raster_metadata(tif_path: Path) -> RasterMeta:
    """Read width, height, geotransform, bounds, resolution, and nodata via gdalinfo -json."""
    result = run_cmd(["gdalinfo", "-json", tif_path], f"gdalinfo {tif_path}")
    info = json.loads(result.stdout)
    width, height = info["size"]
    gt = info["geoTransform"]
    nodata = info["bands"][0].get("noDataValue", -9999.0)
    xmin, ymax = gt[0], gt[3]
    xmax = xmin + width * gt[1]
    ymin = ymax + height * gt[5]
    return RasterMeta(
        width=width,
        height=height,
        geotransform=gt,
        bounds=(xmin, ymin, xmax, ymax),
        res=(abs(gt[1]), abs(gt[5])),
        nodata=nodata,
    )


def _find_best_depth_tif(reach_dir: Path) -> Path | None:
    """Find the best valid depth TIF for a reach.

    Prefers highest numeric z_* directory.
    Falls back to z_nd if no numeric z_* exists.
    Within each z_* dir, tries highest f_* value first (highest flow).
    Skips corrupt files and tries the next candidate.
    """
    numeric_z = []
    z_nd = None
    for d in reach_dir.iterdir():
        if not d.is_dir() or not d.name.startswith("z_"):
            continue
        if d.name == "z_nd":
            z_nd = d
        else:
            try:
                val = float(d.name[2:].replace("_", "."))
                numeric_z.append((val, d))
            except ValueError:
                continue

    numeric_z.sort(key=lambda x: x[0], reverse=True)
    z_dirs = [d for _, d in numeric_z]
    if z_nd is not None:
        z_dirs.append(z_nd)

    for z_dir in z_dirs:
        tifs = []
        for tif in z_dir.glob("f_*.tif"):
            try:
                val = float(tif.stem[2:])
                tifs.append((val, tif))
            except ValueError:
                continue
        tifs.sort(key=lambda x: x[0], reverse=True)

        for _, tif in tifs:
            try:
                run_cmd(["gdalinfo", "-json", tif], f"validate {tif}")
                return tif
            except RuntimeError:
                logger.warning(f"Corrupt TIF, trying next: {tif}")
                continue

    return None


def discover_reaches(library_dir: Path) -> list[tuple[str, Path]]:
    """Walk the library and select the best depth TIF per reach."""
    reaches = []
    for reach_dir in sorted(library_dir.iterdir()):
        if not reach_dir.is_dir():
            continue
        reach_id = reach_dir.name
        tif = _find_best_depth_tif(reach_dir)
        if tif is None:
            logger.warning(f"Reach {reach_id}: no valid depth TIFs found, skipping")
            continue
        logger.info(f"Reach {reach_id}: selected {tif.relative_to(library_dir)}")
        reaches.append((reach_id, tif))
    logger.info(f"Discovered {len(reaches)} reaches with valid depth TIFs")
    return reaches


def _expression_nodata(depth_nodata: float) -> float:
    """Return the nodata value to use in expression VRTs and pre-computed TIFs.

    If depth band has NaN as nodata, substituting a finite sentinel
    keeps the non-bridge (nodata) and bridge-above-water (nan) distinguishable.
    """
    if math.isnan(depth_nodata):
        logger.warning(f"Depth nodata is NaN, using finite sentinel {NODATA_SENTINEL}")
        return NODATA_SENTINEL
    return depth_nodata


def expression_vrt_xml(
    width: int,
    height: int,
    geotransform: list[float],
    nodata: float,
    depth_source: Path,
    clearance_source: Path,
    depth_src_rect: tuple[float, float, float, float] | None = None,
) -> bytes:
    """Build a VRTDerivedRasterBand applying bridge masking to a depth + clearance source pair."""
    expr_nodata = _expression_nodata(nodata)

    root = ET.Element("VRTDataset", rasterXSize=str(width), rasterYSize=str(height))
    ET.SubElement(root, "SRS").text = SRS
    ET.SubElement(root, "GeoTransform").text = ", ".join(repr(v) for v in geotransform)

    band = ET.SubElement(root, "VRTRasterBand", dataType="Float32", band="1", subClass="VRTDerivedRasterBand")
    ET.SubElement(band, "ColorInterp").text = "Gray"
    ET.SubElement(band, "NoDataValue").text = str(expr_nodata)
    ET.SubElement(band, "PixelFunctionType").text = "expression"
    ET.SubElement(band, "PixelFunctionArguments", expression=bridge_expression(expr_nodata), propagateNoData="true")
    ET.SubElement(band, "SkipNonContributingSources").text = "true"

    b1 = ET.SubElement(band, "SimpleSource", name="B1")
    ET.SubElement(b1, "SourceFilename", relativeToVRT="0").text = str(depth_source)
    ET.SubElement(b1, "SourceBand").text = "1"
    if depth_src_rect is not None:
        x_off, y_off, x_size, y_size = depth_src_rect
        ET.SubElement(b1, "SrcRect", xOff=str(x_off), yOff=str(y_off), xSize=str(x_size), ySize=str(y_size))
        ET.SubElement(b1, "DstRect", xOff="0", yOff="0", xSize=str(width), ySize=str(height))

    b2 = ET.SubElement(band, "SimpleSource", name="B2")
    ET.SubElement(b2, "SourceFilename", relativeToVRT="0").text = str(clearance_source)
    ET.SubElement(b2, "SourceBand").text = "1"

    ET.indent(root)
    return ET.tostring(root, encoding="UTF-8")


# ---------------------------------------------------------------------------
# Inner artifact builders
# ---------------------------------------------------------------------------
def _build_expression_vrt(
    inner_dir: Path, name_stem: str, depth_meta: RasterMeta,
    depth_tif: Path, clearance_tif: Path, tile_meta: RasterMeta | None = None,
) -> Path:
    """Build an expression VRT for one clearance source.

    Per-reach (tile_meta=None): VRT covers the full depth extent, no SrcRect.
    Per-tile (tile_meta provided): VRT covers the tile extent with SrcRect into the depth TIF.
    """
    if tile_meta is not None:
        depth_gt = depth_meta.geotransform
        src_x_off = (tile_meta.bounds[0] - depth_gt[0]) / depth_gt[1]
        src_y_off = (depth_gt[3] - tile_meta.bounds[3]) / abs(depth_gt[5])
        depth_src_rect = (src_x_off, src_y_off, tile_meta.width, tile_meta.height)
        w, h, gt = tile_meta.width, tile_meta.height, tile_meta.geotransform
    else:
        depth_src_rect = None
        w, h, gt = depth_meta.width, depth_meta.height, depth_meta.geotransform

    vrt_path = inner_dir / f"{name_stem}.vrt"
    vrt_path.write_bytes(
        expression_vrt_xml(w, h, gt, depth_meta.nodata, depth_tif, clearance_tif, depth_src_rect)
    )
    return vrt_path


def _build_precomputed_tif(
    inner_dir: Path, name_stem: str, depth_tif: Path, clearance_tif: Path, nodata: float,
) -> Path:
    """Pre-compute a masked depth TIF for one clearance source using gdal_calc."""
    masked_tif = inner_dir / f"{name_stem}.tif"
    run_cmd(
        [
            "gdal_calc", "-A", depth_tif, "-B", clearance_tif,
            "--outfile", masked_tif, f"--NoDataValue={nodata}",
            "--extent=intersect", "--co=COMPRESS=LZW", "--quiet",
            "--calc=numpy.where(A - B >= 0, A - B, numpy.nan)",
        ],
        f"gdal_calc masked {name_stem}",
    )
    return masked_tif


# ---------------------------------------------------------------------------
# Source collection
# ---------------------------------------------------------------------------
def _find_per_reach_clearance(reach_dir: Path) -> Path | None:
    candidate = reach_dir / "bridge_heights" / "combined.tif"
    return candidate if candidate.is_file() else None


def _find_per_tile_clearances(reach_dir: Path) -> list[Path]:
    bridge_dir = reach_dir / "bridge_heights"
    if not bridge_dir.is_dir():
        return []
    return sorted(p for p in bridge_dir.glob("*.tif") if p.name != "combined.tif")


def _validate_tile_alignment(reach_id: str, tile_tif: Path, tile_meta: RasterMeta, depth_meta: RasterMeta) -> bool:
    """Check that a bridge tile is aligned with its depth TIF. Returns False to skip."""
    tile_gt = tile_meta.geotransform
    if tile_gt[2] != 0 or tile_gt[4] != 0:
        logger.error(f"Reach {reach_id}, tile {tile_tif.name}: non-zero rotation terms, skipping")
        return False

    if abs(tile_meta.res[0] - depth_meta.res[0]) > 1e-10 or abs(tile_meta.res[1] - depth_meta.res[1]) > 1e-10:
        logger.error(
            f"Reach {reach_id}, tile {tile_tif.name}: resolution mismatch "
            f"(depth: {depth_meta.res}, tile: {tile_meta.res}), skipping"
        )
        return False

    tb, db = tile_meta.bounds, depth_meta.bounds
    if tb[0] < db[0] - 1e-6 or tb[1] < db[1] - 1e-6 or tb[2] > db[2] + 1e-6 or tb[3] > db[3] + 1e-6:
        logger.error(f"Reach {reach_id}, tile {tile_tif.name}: tile extends outside depth raster bounds, skipping")
        return False

    return True


def collect_sources(
    library_dir: Path, reaches: list[tuple[str, Path]], inner_dir: Path,
    granularity: str, approach: str,
) -> tuple[list[Path], list[Path], list[str]]:
    """Collect depth and bridge sources for one combination.

    granularity: "per_reach" (one combined.tif per reach) or "per_tile" (one per bridge tile)
    approach: "expression" (VRT with muparser) or "precomputed" (gdal_calc TIF)
    """
    depth_sources = []
    bridge_sources = []
    masking_failures = []

    for reach_id, depth_tif in reaches:
        depth_sources.append(depth_tif)

        if granularity == "per_reach":
            clearance = _find_per_reach_clearance(library_dir / reach_id)
            items = [(clearance, reach_id)] if clearance else []
        else:
            items = [(t, f"{reach_id}_{t.stem}") for t in _find_per_tile_clearances(library_dir / reach_id)]

        if not items:
            continue

        logger.info(f"Reach {reach_id}: {len(items)} clearance item(s), {approach} {granularity}")

        try:
            depth_meta = raster_metadata(depth_tif)
        except Exception:
            logger.exception(f"Reach {reach_id}: failed to read depth metadata")
            masking_failures.append(reach_id)
            continue

        if granularity == "per_tile":
            depth_gt = depth_meta.geotransform
            if depth_gt[2] != 0 or depth_gt[4] != 0:
                logger.error(f"Reach {reach_id}: non-zero rotation, skipping bridge masking")
                masking_failures.append(reach_id)
                continue

        expr_nodata = _expression_nodata(depth_meta.nodata)

        for clearance_tif, name_stem in items:
            try:
                tile_meta = None
                if granularity == "per_tile":
                    tile_meta = raster_metadata(clearance_tif)
                    if not _validate_tile_alignment(reach_id, clearance_tif, tile_meta, depth_meta):
                        masking_failures.append(reach_id)
                        continue

                if approach == "expression":
                    path = _build_expression_vrt(
                        inner_dir, name_stem, depth_meta, depth_tif, clearance_tif, tile_meta
                    )
                else:
                    path = _build_precomputed_tif(
                        inner_dir, name_stem, depth_tif, clearance_tif, expr_nodata
                    )
                bridge_sources.append(path)
            except Exception:
                masking_failures.append(reach_id)
                logger.exception(f"Reach {reach_id}: failed for {clearance_tif.name}")

    return depth_sources, bridge_sources, masking_failures


# ---------------------------------------------------------------------------
# Composite builders
# ---------------------------------------------------------------------------
def _write_source_list(sources: list, list_path: Path, quote: bool = False) -> None:
    """Write source paths to a file, one per line."""
    if quote:
        list_path.write_text("\n".join(f'"{s}"' for s in sources))
    else:
        list_path.write_text("\n".join(str(s) for s in sources))


def _inline_vrt_sources(tree: ET.ElementTree, outer_vrt_path: Path) -> int:
    """Replace SourceFilename references to .vrt files with inline VRTDataset content."""
    inlined = 0
    root = tree.getroot()
    for source in root.iter():
        if source.tag not in ("SimpleSource", "ComplexSource"):
            continue
        src_filename = source.find("SourceFilename")
        if src_filename is None or not src_filename.text.endswith(".vrt"):
            continue
        if src_filename.get("relativeToVRT") == "1":
            vrt_path = outer_vrt_path.parent / src_filename.text
        else:
            vrt_path = Path(src_filename.text)
        if not vrt_path.is_file():
            logger.warning(f"Inner VRT not found, skipping inline: {vrt_path}")
            continue
        inner_root = ET.parse(vrt_path).getroot()
        source.remove(src_filename)
        source.insert(0, inner_root)
        inlined += 1
    return inlined


def build_outer_vrt(sources: list[Path], out_path: Path, inline: bool = False) -> None:
    """Mosaic sources into a composite VRT."""
    if not sources:
        raise ValueError(f"{out_path}: no sources to build composite VRT")
    list_file = out_path.parent / "source_list.txt"
    try:
        _write_source_list(sources, list_file)
        run_cmd(
            ["gdalbuildvrt", "-overwrite", "-input_file_list", list_file, out_path],
            f"gdalbuildvrt {out_path}",
        )
    finally:
        list_file.unlink(missing_ok=True)

    if inline:
        tree = ET.parse(out_path)
        inlined = _inline_vrt_sources(tree, out_path)
        ET.indent(tree)
        tree.write(out_path, encoding="UTF-8", xml_declaration=False)
        logger.info(f"{out_path}: {len(sources)} sources, {inlined} inlined")
    else:
        logger.info(f"{out_path}: {len(sources)} sources")


def build_outer_gti(sources: list, out_path: Path) -> None:
    """Build a GeoPackage tile index (GTI) from sources with gdaltindex."""
    if not sources:
        raise ValueError(f"{out_path}: no sources to build tile index")
    list_file = out_path.parent / "source_list.txt"
    try:
        _write_source_list(sources, list_file, quote=True)
        run_cmd(
            ["gdaltindex", "-f", "GPKG", "-lyr_name", GTI_LAYER_NAME, "-overwrite", out_path, "--optfile", list_file],
            f"gdaltindex {out_path}",
        )
    finally:
        list_file.unlink(missing_ok=True)

    combo_dir_str = str(out_path.parent) + os.sep
    run_cmd(
        ["ogrinfo", out_path, "-sql",
         f"UPDATE {GTI_LAYER_NAME} SET location = REPLACE(location, '{combo_dir_str}', '')"],
        "relativize inner paths",
    )

    logger.info(f"{out_path}: {len(sources)} sources")


def _materialize(src_path: Path, out_path: Path) -> None:
    """Materialize a VRT/GTI to a compressed GeoTIFF (needed when QGIS lacks muparser)."""
    run_cmd(
        ["gdal_translate", "-of", "GTiff", "-co", "COMPRESS=LZW", src_path, out_path],
        f"materialize {out_path.name}",
    )
    logger.info(f"Materialized to {out_path} ({out_path.stat().st_size / 1024 / 1024:.1f} MB)")


def build_combo(
    depth_sources: list[Path], bridge_sources: list[Path], combo_dir: Path,
    outer_kind: str, is_expression: bool = False,
) -> None:
    if outer_kind == "vrt":
        vrt_path = combo_dir / "composite.vrt"
        build_outer_vrt(depth_sources + bridge_sources, vrt_path, inline=is_expression)
        if is_expression:
            inner = combo_dir / "inner_vrts"
            if inner.is_dir():
                shutil.rmtree(inner, ignore_errors=True)
            _materialize(vrt_path, combo_dir / "composite.tif")
    else:
        gti_path = combo_dir / "composite.gti.gpkg"
        gti_sources = [str(s) for s in depth_sources + bridge_sources]
        build_outer_gti(gti_sources, gti_path)
        if is_expression:
            _materialize(gti_path, combo_dir / "composite.tif")


# ---------------------------------------------------------------------------
# S3 path remapping
# ---------------------------------------------------------------------------
def remap_to_s3(output_dir: Path, local_library_str: str, s3_library: str) -> None:
    """Replace local library paths with /vsis3/ paths in all VRT and GTI files."""
    local_fwd = local_library_str.replace("\\", "/")

    for vrt in output_dir.rglob("*.vrt"):
        content = vrt.read_text(encoding="utf-8")
        content = content.replace(local_library_str, s3_library)
        content = content.replace(local_fwd, s3_library)
        content = content.replace("\\", "/")
        vrt.write_text(content, encoding="utf-8")
    logger.info(f"Remapped library paths in {sum(1 for _ in output_dir.rglob('*.vrt'))} VRT file(s)")

    for gpkg in output_dir.rglob("*.gpkg"):
        run_cmd(
            ["ogrinfo", gpkg, "-sql",
             f"UPDATE {GTI_LAYER_NAME} SET location = REPLACE(location, '{local_library_str}', '{s3_library}')"],
            f"remap library paths in {gpkg.name}",
        )
        run_cmd(
            ["ogrinfo", gpkg, "-sql",
             f"UPDATE {GTI_LAYER_NAME} SET location = REPLACE(location, char(92), '/')"],
            f"fix backslashes in {gpkg.name}",
        )
    logger.info(f"Remapped paths in {sum(1 for _ in output_dir.rglob('*.gpkg'))} GTI file(s)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate 8 VRT/GTI matrix combinations for bridge masking profiling"
    )
    parser.add_argument("--library", required=True, help="Path to library dir with reach subdirs")
    parser.add_argument("--output", required=True, help="Output directory for the 8 combos")
    parser.add_argument(
        "--s3-library", default=None,
        help="If set, remap library paths to this /vsis3/ prefix for cross-platform use "
             "(e.g., /vsis3/bucket/collection/library)",
    )
    args = parser.parse_args()

    library_dir = Path(args.library).resolve()
    output_dir = Path(args.output).resolve()

    if not library_dir.is_dir():
        raise FileNotFoundError(f"Library directory not found: {library_dir}")

    logger.info(f"Library: {library_dir}")
    logger.info(f"Output: {output_dir}")

    reaches = discover_reaches(library_dir)
    if not reaches:
        logger.error("No reaches with valid depth TIFs found in library")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "selected_depth_tifs.txt"
    manifest_path.write_text("\n".join(str(tif) for _, tif in reaches))
    logger.info(f"Selected depth TIFs written to {manifest_path}")

    combos = [
        ("expression_per_reach_vrt", "per_reach", "expression", "vrt"),
        ("expression_per_reach_gti", "per_reach", "expression", "gti"),
        ("expression_per_tile_vrt", "per_tile", "expression", "vrt"),
        ("expression_per_tile_gti", "per_tile", "expression", "gti"),
        ("precomputed_per_reach_vrt", "per_reach", "precomputed", "vrt"),
        ("precomputed_per_reach_gti", "per_reach", "precomputed", "gti"),
        ("precomputed_per_tile_vrt", "per_tile", "precomputed", "vrt"),
        ("precomputed_per_tile_gti", "per_tile", "precomputed", "gti"),
    ]

    failures = []
    all_masking_failures = []

    for combo_name, granularity, approach, outer_kind in combos:
        logger.info(f"=== {combo_name} ===")
        combo_dir = output_dir / combo_name
        is_expression = approach == "expression"
        inner_name = "inner_vrts" if is_expression else "inner_tifs"
        inner_dir = combo_dir / inner_name
        inner_dir.mkdir(parents=True, exist_ok=True)
        try:
            depth_sources, bridge_sources, masking_fails = collect_sources(
                library_dir, reaches, inner_dir, granularity, approach
            )
            all_masking_failures.extend(masking_fails)

            if not depth_sources and not bridge_sources:
                logger.error(f"{combo_name}: no sources collected")
                failures.append(combo_name)
                continue

            build_combo(
                depth_sources, bridge_sources, combo_dir, outer_kind,
                is_expression=is_expression,
            )
        except Exception:
            logger.exception(f"Failed to build {combo_name}")
            failures.append(combo_name)

    if args.s3_library:
        logger.info("=== Remapping paths to S3 ===")
        remap_to_s3(output_dir, str(library_dir), args.s3_library)

    if all_masking_failures:
        unique = sorted(set(all_masking_failures))
        logger.warning(f"Reaches with bridge masking failures (included unmasked): {', '.join(unique)}")

    if failures:
        logger.error(f"Failed combos: {', '.join(failures)}")
        sys.exit(1)

    logger.info(f"Done. 8 combos written under {output_dir}")


if __name__ == "__main__":
    main()
