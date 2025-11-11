"""
observability/monitor_dashboard.py
Command-line dashboard for pipeline monitoring
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path("conf/.env"))

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_DB = os.getenv("PG_DB", "uganda_health")

ENGINE = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    echo=False
)


class MonitoringDashboard:
    """
    Display pipeline health metrics and recent activity
    """

    def __init__(self, engine=None):
        self.engine = engine or ENGINE

    def show_recent_runs(self, limit=10):
        """Show recent pipeline runs"""
        query = """
        SELECT
            run_id,
            pipeline_name,
            pipeline_stage,
            status,
            started_at,
            COALESCE(execution_duration_seconds, 0) as duration_sec,
            records_processed,
            records_loaded
        FROM metadata.pipeline_runs
        ORDER BY started_at DESC
        LIMIT :limit
        """

        df = pd.read_sql(text(query), self.engine, params={"limit": limit})

        print("\n" + "="*100)
        print("RECENT PIPELINE RUNS")
        print("="*100)

        if df.empty:
            print("No pipeline runs found.")
            return

        for _, row in df.iterrows():
            status_icon = "[OK]" if row['status'] == 'success' else "[FAIL]"
            print(f"{status_icon} {row['pipeline_name']}/{row['pipeline_stage']}")
            print(f"   ID: {row['run_id']}")
            print(f"   Started: {row['started_at']}")
            print(f"   Duration: {row['duration_sec']:.1f}s")
            print(f"   Records: {row['records_processed']} processed -> {row['records_loaded']} loaded")
            print(f"   Status: {row['status']}")
            print()

    def show_pipeline_health(self):
        """Show pipeline success rates"""
        query = """
        SELECT * FROM metadata.v_pipeline_health
        ORDER BY last_run_at DESC
        """

        df = pd.read_sql(query, self.engine)

        print("\n" + "="*100)
        print("PIPELINE HEALTH (Last 30 Days)")
        print("="*100)

        if df.empty:
            print("No pipeline health data available.")
            return

        print(f"{'Pipeline':<30} {'Runs':<8} {'Success':^10} {'Failed':^10} {'Success Rate':^15} {'Avg Duration':^15}")
        print("-" * 100)

        for _, row in df.iterrows():
            success_rate = row['success_rate'] if pd.notna(row['success_rate']) else 0
            avg_duration = row['avg_duration_seconds'] if pd.notna(row['avg_duration_seconds']) else 0

            status_indicator = "[OK]" if success_rate >= 95 else "[WARN]" if success_rate >= 80 else "[FAIL]"

            print(f"{row['pipeline_name']:<30} "
                  f"{row['total_runs']:<8} "
                  f"{row['successful_runs']:^10} "
                  f"{row['failed_runs']:^10} "
                  f"{status_indicator} {success_rate:>5.1f}%       "
                  f"{avg_duration:>6.1f}s")

        print()

    def show_data_quality_summary(self):
        """Show data quality metrics"""
        query = """
        SELECT
            check_category,
            COUNT(*) as total_checks,
            SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks,
            ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_rate
        FROM metadata.data_quality_metrics
        WHERE checked_at > NOW() - INTERVAL '7 days'
        GROUP BY check_category
        ORDER BY check_category
        """

        df = pd.read_sql(query, self.engine)

        print("\n" + "="*100)
        print("DATA QUALITY SUMMARY (Last 7 Days)")
        print("="*100)

        if df.empty:
            print("No quality checks recorded.")
            return

        print(f"{'Category':<20} {'Total Checks':^15} {'Passed':^15} {'Pass Rate':^15}")
        print("-" * 100)

        for _, row in df.iterrows():
            pass_rate = row['pass_rate'] if pd.notna(row['pass_rate']) else 0
            status = "[OK]" if pass_rate >= 95 else "[WARN]" if pass_rate >= 80 else "[FAIL]"

            print(f"{row['check_category']:<20} "
                  f"{row['total_checks']:^15} "
                  f"{row['passed_checks']:^15} "
                  f"{status} {pass_rate:>6.1f}%")

        print()

    def show_failed_quality_checks(self, limit=10):
        """Show recent failed quality checks"""
        query = """
        SELECT * FROM metadata.v_failed_quality_checks
        LIMIT :limit
        """

        df = pd.read_sql(text(query), self.engine, params={"limit": limit})

        print("\n" + "="*100)
        print(f"RECENT FAILED QUALITY CHECKS (Top {limit})")
        print("="*100)

        if df.empty:
            print("[OK] No failed quality checks!")
            return

        for _, row in df.iterrows():
            print(f"[FAIL] {row['check_name']} ({row['check_category']})")
            print(f"   Table: {row['table_name']}")
            if pd.notna(row['column_name']):
                print(f"   Column: {row['column_name']}")
            print(f"   Metric: {row['metric_value']} (threshold: {row['threshold_value']})")
            print(f"   Source: {row['source_file']}")
            print(f"   Time: {row['checked_at']}")
            if pd.notna(row['details']):
                print(f"   Details: {row['details']}")
            print()

    def show_lineage(self, table_name, column_name=None):
        """Show field lineage for a table/column"""
        if column_name:
            query = """
            SELECT * FROM metadata.get_field_lineage(:table, :column)
            """
            df = pd.read_sql(text(query), self.engine, params={"table": table_name, "column": column_name})
            title = f"LINEAGE: {table_name}.{column_name}"
        else:
            query = """
            SELECT DISTINCT
                target_column,
                source_file,
                source_column,
                transformation_type,
                transformation_logic
            FROM metadata.field_lineage
            WHERE target_table = :table
            ORDER BY target_column
            """
            df = pd.read_sql(text(query), self.engine, params={"table": table_name})
            title = f"LINEAGE: {table_name} (all columns)"

        print("\n" + "="*100)
        print(title)
        print("="*100)

        if df.empty:
            print("No lineage information found.")
            return

        for _, row in df.iterrows():
            if 'target_column' in row:
                print(f"\nColumn: {row['target_column']}")
            print(f"  Source: {row['source_file']} -> {row['source_column']}")
            if 'transformation_type' in row:
                print(f"  Type: {row['transformation_type']}")
            print(f"  Logic: {row['transformation_logic']}")

        print()

    def show_source_files(self):
        """Show registered source files"""
        query = """
        SELECT
            file_name,
            sheet_count,
            row_count,
            processing_count,
            first_seen,
            last_processed,
            status
        FROM metadata.source_files
        ORDER BY last_processed DESC NULLS LAST
        """

        df = pd.read_sql(query, self.engine)

        print("\n" + "="*100)
        print("SOURCE FILES")
        print("="*100)

        if df.empty:
            print("No source files registered.")
            return

        for _, row in df.iterrows():
            status_icon = "[OK]" if row['status'] == 'processed' else "[FAIL]" if row['status'] == 'failed' else "[PENDING]"
            print(f"{status_icon} {row['file_name']}")
            print(f"   Sheets: {row['sheet_count']}, Rows: {row['row_count']}")
            print(f"   Processed: {row['processing_count']} times")
            print(f"   First seen: {row['first_seen']}")
            print(f"   Last processed: {row['last_processed']}")
            print()

    def show_full_dashboard(self):
        """Show complete dashboard"""
        print("\n" + "="*100)
        print(" "*30 + "UGANDA HEALTH ETL PIPELINE DASHBOARD")
        print("="*100)
        print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        self.show_pipeline_health()
        self.show_data_quality_summary()
        self.show_recent_runs(limit=5)
        self.show_failed_quality_checks(limit=5)
        self.show_source_files()

        print("\n" + "="*100)
        print("For detailed lineage, run: python observability/monitor_dashboard.py lineage <table_name> [column_name]")
        print("="*100 + "\n")


def main():
    """CLI interface for monitoring dashboard"""
    dashboard = MonitoringDashboard()

    if len(sys.argv) == 1:
        # No arguments - show full dashboard
        dashboard.show_full_dashboard()

    elif sys.argv[1] == 'health':
        dashboard.show_pipeline_health()

    elif sys.argv[1] == 'quality':
        dashboard.show_data_quality_summary()
        dashboard.show_failed_quality_checks(limit=20)

    elif sys.argv[1] == 'runs':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        dashboard.show_recent_runs(limit=limit)

    elif sys.argv[1] == 'lineage':
        if len(sys.argv) < 3:
            print("Usage: python observability/monitor_dashboard.py lineage <table_name> [column_name]")
            sys.exit(1)
        table_name = sys.argv[2]
        column_name = sys.argv[3] if len(sys.argv) > 3 else None
        dashboard.show_lineage(table_name, column_name)

    elif sys.argv[1] == 'files':
        dashboard.show_source_files()

    else:
        print("Usage: python observability/monitor_dashboard.py [command]")
        print("\nCommands:")
        print("  (no command)  - Show full dashboard")
        print("  health        - Show pipeline health")
        print("  quality       - Show data quality summary")
        print("  runs [N]      - Show recent N pipeline runs")
        print("  lineage <table> [column] - Show data lineage")
        print("  files         - Show source files")


if __name__ == "__main__":
    main()
