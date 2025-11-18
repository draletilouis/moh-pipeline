#!/usr/bin/env python
"""
Uganda Health Pipeline Orchestrator
Runs all pipeline stages in sequence: Ingestion -> Transform -> Load
"""
import sys
import logging
from pathlib import Path
from datetime import datetime
import importlib.util

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def import_and_run_module(module_path: str, module_name: str):
    """
    Import and run a module's main() function

    Args:
        module_path: Path to the Python module file
        module_name: Name for the module
    """
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    if hasattr(module, 'main'):
        module.main()
    else:
        raise AttributeError(f"Module {module_name} does not have a 'main()' function")


def run_full_pipeline():
    """Run complete ETL pipeline"""
    logger.info("=" * 70)
    logger.info("UGANDA HEALTH PIPELINE - STARTING")
    logger.info("=" * 70)

    start_time = datetime.now()

    try:
        # Stage 1: Ingestion
        logger.info("")
        logger.info("=" * 70)
        logger.info("STAGE 1: INGESTION - Loading Excel data to CSV")
        logger.info("=" * 70)
        stage1_start = datetime.now()

        import_and_run_module(
            "ingestion/load-excel.py",
            "ingestion_load_excel"
        )

        stage1_duration = (datetime.now() - stage1_start).total_seconds()
        logger.info(f"✅ Ingestion completed in {stage1_duration:.2f} seconds")

        # Stage 2: Transform
        logger.info("")
        logger.info("=" * 70)
        logger.info("STAGE 2: TRANSFORM - Cleaning and unpivoting data")
        logger.info("=" * 70)
        stage2_start = datetime.now()

        from transform.clean_and_unpivot import main as transform_main
        transform_main()

        stage2_duration = (datetime.now() - stage2_start).total_seconds()
        logger.info(f"✅ Transform completed in {stage2_duration:.2f} seconds")

        # Stage 3: Warehouse Load
        logger.info("")
        logger.info("=" * 70)
        logger.info("STAGE 3: WAREHOUSE LOAD - Loading to PostgreSQL")
        logger.info("=" * 70)
        stage3_start = datetime.now()

        from warehouse.load_to_postgres import main as warehouse_main
        warehouse_main()

        stage3_duration = (datetime.now() - stage3_start).total_seconds()
        logger.info(f"✅ Warehouse load completed in {stage3_duration:.2f} seconds")

        # Pipeline completion summary
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info("")
        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"Stage 1 (Ingestion):  {stage1_duration:.2f}s")
        logger.info(f"Stage 2 (Transform):  {stage2_duration:.2f}s")
        logger.info(f"Stage 3 (Load):       {stage3_duration:.2f}s")
        logger.info(f"Total Duration:       {total_duration:.2f}s")
        logger.info("=" * 70)

        return 0

    except Exception as e:
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.error("")
        logger.error("=" * 70)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error(f"Error: {str(e)}")
        logger.error(f"Failed after {total_duration:.2f} seconds")
        logger.error("=" * 70)
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    exit_code = run_full_pipeline()
    sys.exit(exit_code)