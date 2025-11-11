"""
observability/pipeline_observer.py
Tracks pipeline execution, data quality, and lineage
"""

import os
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from uuid import UUID
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path("conf/.env"))


class PipelineObserver:
    """
    Tracks pipeline execution with comprehensive observability.

    Features:
    - Run tracking (start, complete, fail)
    - Data quality metrics logging
    - Field lineage tracking
    - Source file registration
    """

    def __init__(self, db_engine=None):
        """Initialize with database connection"""
        if db_engine is None:
            PG_HOST = os.getenv("PG_HOST", "localhost")
            PG_PORT = os.getenv("PG_PORT", "5432")
            PG_USER = os.getenv("PG_USER", "postgres")
            PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
            PG_DB = os.getenv("PG_DB", "uganda_health")

            db_engine = create_engine(
                f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
                echo=False
            )

        self.engine = db_engine
        self.run_id = None
        self.started_at = None
        self.pipeline_name = None
        self.pipeline_stage = None

    def start_run(
        self,
        pipeline_name: str,
        pipeline_stage: str,
        source_file: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> UUID:
        """
        Start tracking a pipeline run

        Args:
            pipeline_name: Name of the pipeline (e.g., 'uganda_health_etl')
            pipeline_stage: Stage name ('ingestion', 'transform', 'load')
            source_file: Path to source file being processed
            metadata: Additional context as dict

        Returns:
            run_id: UUID of this run
        """
        self.pipeline_name = pipeline_name
        self.pipeline_stage = pipeline_stage
        self.started_at = datetime.now()

        with self.engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO metadata.pipeline_runs
                (pipeline_name, pipeline_stage, source_file, status, metadata)
                VALUES (:name, :stage, :file, 'running', :meta)
                RETURNING run_id
            """), {
                "name": pipeline_name,
                "stage": pipeline_stage,
                "file": str(source_file) if source_file else None,
                "meta": json.dumps(metadata) if metadata else None
            })
            self.run_id = result.fetchone()[0]

        print(f"[OBSERVABILITY] Started run {self.run_id} - {pipeline_name}/{pipeline_stage}")
        return self.run_id

    def complete_run(
        self,
        status: str = 'success',
        records_input: Optional[int] = None,
        records_processed: Optional[int] = None,
        records_loaded: Optional[int] = None,
        records_rejected: Optional[int] = None,
        error_message: Optional[str] = None,
        error_details: Optional[Dict] = None
    ):
        """
        Mark run as complete with statistics

        Args:
            status: 'success', 'failed', or 'skipped'
            records_input: Number of records read from source
            records_processed: Number of records processed
            records_loaded: Number of records loaded to target
            records_rejected: Number of records rejected
            error_message: Error message if failed
            error_details: Additional error context
        """
        if not self.run_id:
            raise ValueError("No active run to complete. Call start_run() first.")

        completed_at = datetime.now()
        duration = (completed_at - self.started_at).total_seconds()

        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE metadata.pipeline_runs
                SET
                    status = :status,
                    completed_at = :completed,
                    records_input = :input,
                    records_processed = :processed,
                    records_loaded = :loaded,
                    records_rejected = :rejected,
                    execution_duration_seconds = :duration,
                    error_message = :error,
                    error_details = :details
                WHERE run_id = :run_id
            """), {
                "run_id": self.run_id,
                "status": status,
                "completed": completed_at,
                "input": records_input,
                "processed": records_processed,
                "loaded": records_loaded,
                "rejected": records_rejected,
                "duration": duration,
                "error": error_message,
                "details": json.dumps(error_details) if error_details else None
            })

        status_icon = "[SUCCESS]" if status == 'success' else "[FAILED]"
        print(f"[OBSERVABILITY] {status_icon} Run {self.run_id} completed: {status} ({duration:.2f}s)")

        if records_processed:
            print(f"  Records: {records_processed} processed, {records_loaded or 0} loaded")

    def log_quality_check(
        self,
        check_name: str,
        passed: bool,
        check_category: str = 'general',
        table_name: Optional[str] = None,
        column_name: Optional[str] = None,
        metric_value: Optional[float] = None,
        threshold_value: Optional[float] = None,
        row_count: Optional[int] = None,
        failure_count: Optional[int] = None,
        details: Optional[Dict] = None
    ):
        """
        Log a data quality check result

        Args:
            check_name: Name of the check (e.g., 'completeness_value')
            passed: Whether check passed
            check_category: 'completeness', 'validity', 'consistency', 'timeliness'
            table_name: Table being checked
            column_name: Column being checked
            metric_value: Actual value measured
            threshold_value: Threshold for passing
            row_count: Total rows checked
            failure_count: Number of rows that failed
            details: Additional context
        """
        if not self.run_id:
            raise ValueError("No active run. Call start_run() first.")

        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO metadata.data_quality_metrics
                (run_id, check_name, check_category, table_name, column_name,
                 passed, metric_value, threshold_value, row_count, failure_count, details)
                VALUES (:run_id, :check, :category, :table, :column,
                        :passed, :metric, :threshold, :rows, :failures, :details)
            """), {
                "run_id": self.run_id,
                "check": check_name,
                "category": check_category,
                "table": table_name,
                "column": column_name,
                "passed": bool(passed),  # Convert numpy.bool_ to Python bool
                "metric": float(metric_value) if metric_value is not None else None,
                "threshold": float(threshold_value) if threshold_value is not None else None,
                "rows": int(row_count) if row_count is not None else None,
                "failures": int(failure_count) if failure_count is not None else None,
                "details": json.dumps(details) if details else None
            })

        status = "[PASS]" if passed else "[FAIL]"
        print(f"[QUALITY] {status} - {check_name}: {metric_value}")

    def track_lineage(
        self,
        target_table: str,
        target_column: str,
        source_file: str,
        source_column: str,
        transformation_logic: str,
        transformation_type: str = 'direct_copy',
        source_sheet: Optional[str] = None
    ):
        """
        Track field-level lineage

        Args:
            target_table: Target table in warehouse
            target_column: Target column name
            source_file: Source file path
            source_column: Source column name
            transformation_logic: Description of transformation
            transformation_type: 'direct_copy', 'unpivot', 'aggregate', 'derived'
            source_sheet: Sheet name in Excel (if applicable)
        """
        if not self.run_id:
            raise ValueError("No active run. Call start_run() first.")

        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO metadata.field_lineage
                (run_id, target_table, target_column, source_file, source_sheet,
                 source_column, transformation_logic, transformation_type)
                VALUES (:run_id, :table, :col, :file, :sheet, :src_col, :logic, :type)
            """), {
                "run_id": self.run_id,
                "table": target_table,
                "col": target_column,
                "file": source_file,
                "sheet": source_sheet,
                "src_col": source_column,
                "logic": transformation_logic,
                "type": transformation_type
            })

    def register_source_file(
        self,
        file_path: str,
        sheet_count: Optional[int] = None,
        row_count: Optional[int] = None,
        column_count: Optional[int] = None,
        schema_fingerprint: Optional[Dict] = None
    ) -> int:
        """
        Register a source file in the catalog

        Args:
            file_path: Full path to source file
            sheet_count: Number of sheets in Excel
            row_count: Total rows
            column_count: Total columns
            schema_fingerprint: Schema metadata as dict

        Returns:
            file_id: Database ID of registered file
        """
        path = Path(file_path)
        file_hash = self._compute_file_hash(path)
        file_size = path.stat().st_size if path.exists() else None

        with self.engine.begin() as conn:
            # Check if file already exists
            result = conn.execute(text("""
                SELECT file_id, file_hash
                FROM metadata.source_files
                WHERE file_path = :path
            """), {"path": str(file_path)})

            existing = result.fetchone()

            if existing:
                file_id, existing_hash = existing
                if existing_hash == file_hash:
                    # File unchanged - update last_processed
                    conn.execute(text("""
                        UPDATE metadata.source_files
                        SET last_processed = NOW(),
                            processing_count = processing_count + 1
                        WHERE file_id = :id
                    """), {"id": file_id})
                    print(f"[OBSERVABILITY] File unchanged: {path.name}")
                    return file_id
                else:
                    # File changed - update
                    conn.execute(text("""
                        UPDATE metadata.source_files
                        SET file_hash = :hash,
                            file_size_bytes = :size,
                            last_processed = NOW(),
                            processing_count = processing_count + 1,
                            schema_fingerprint = :schema,
                            row_count = :rows,
                            column_count = :cols,
                            sheet_count = :sheets
                        WHERE file_id = :id
                    """), {
                        "id": file_id,
                        "hash": file_hash,
                        "size": file_size,
                        "schema": json.dumps(schema_fingerprint) if schema_fingerprint else None,
                        "rows": row_count,
                        "cols": column_count,
                        "sheets": sheet_count
                    })
                    print(f"[OBSERVABILITY] File changed: {path.name}")
                    return file_id
            else:
                # New file - insert
                result = conn.execute(text("""
                    INSERT INTO metadata.source_files
                    (file_path, file_name, file_hash, file_size_bytes,
                     sheet_count, row_count, column_count, schema_fingerprint, status)
                    VALUES (:path, :name, :hash, :size, :sheets, :rows, :cols, :schema, 'processed')
                    RETURNING file_id
                """), {
                    "path": str(file_path),
                    "name": path.name,
                    "hash": file_hash,
                    "size": file_size,
                    "sheets": sheet_count,
                    "rows": row_count,
                    "cols": column_count,
                    "schema": json.dumps(schema_fingerprint) if schema_fingerprint else None
                })
                file_id = result.fetchone()[0]
                print(f"[OBSERVABILITY] Registered new file: {path.name}")
                return file_id

    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute MD5 hash of file for change detection"""
        if not file_path.exists():
            return "file_not_found"

        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    def get_quality_score(self) -> Optional[float]:
        """Get data quality score for current run (0-100)"""
        if not self.run_id:
            return None

        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT metadata.get_quality_score(:run_id)
            """), {"run_id": self.run_id})
            return result.scalar()

    def print_summary(self):
        """Print summary of current run"""
        if not self.run_id:
            print("[OBSERVABILITY] No active run")
            return

        with self.engine.connect() as conn:
            # Get run details
            run = conn.execute(text("""
                SELECT pipeline_name, pipeline_stage, status,
                       records_processed, records_loaded, records_rejected,
                       execution_duration_seconds
                FROM metadata.pipeline_runs
                WHERE run_id = :run_id
            """), {"run_id": self.run_id}).fetchone()

            # Get quality checks
            quality = conn.execute(text("""
                SELECT check_category, COUNT(*) as total,
                       SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed
                FROM metadata.data_quality_metrics
                WHERE run_id = :run_id
                GROUP BY check_category
            """), {"run_id": self.run_id}).fetchall()

        print("\n" + "="*60)
        print(f"PIPELINE RUN SUMMARY: {self.run_id}")
        print("="*60)
        print(f"Pipeline: {run[0]} / {run[1]}")
        print(f"Status: {run[2]}")
        print(f"Duration: {run[6]:.2f}s")
        print(f"Records: {run[3]} processed -> {run[4]} loaded ({run[5] or 0} rejected)")

        if quality:
            print("\nData Quality Checks:")
            for category, total, passed in quality:
                pct = (passed / total * 100) if total > 0 else 0
                print(f"  {category}: {passed}/{total} passed ({pct:.1f}%)")

        quality_score = self.get_quality_score()
        if quality_score is not None:
            print(f"\nOverall Quality Score: {quality_score:.1f}/100")

        print("="*60 + "\n")


# Context manager for automatic run tracking
class ObservedPipeline:
    """
    Context manager for automatic pipeline tracking

    Usage:
        with ObservedPipeline('etl', 'transform', source_file='data.csv') as observer:
            # do work
            observer.log_quality_check('completeness', True, metric_value=0.99)
    """

    def __init__(
        self,
        pipeline_name: str,
        pipeline_stage: str,
        source_file: Optional[str] = None,
        auto_print_summary: bool = True
    ):
        self.observer = PipelineObserver()
        self.pipeline_name = pipeline_name
        self.pipeline_stage = pipeline_stage
        self.source_file = source_file
        self.auto_print_summary = auto_print_summary

    def __enter__(self) -> PipelineObserver:
        self.observer.start_run(
            self.pipeline_name,
            self.pipeline_stage,
            self.source_file
        )
        return self.observer

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Success
            self.observer.complete_run(status='success')
        else:
            # Failed
            self.observer.complete_run(
                status='failed',
                error_message=str(exc_val),
                error_details={'exception_type': exc_type.__name__}
            )

        if self.auto_print_summary:
            self.observer.print_summary()

        # Don't suppress exceptions
        return False
