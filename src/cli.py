import sys
import uuid
import click

from src.extract.extract import extract_sources
from src.transform.staging import run_staging
from src.transform.curated import run_curated
from src.load.load import load_curated, load_partition as load_partition_
from src.validate.validate import validate_curated, ValidationError
from src.benchmark.benchmark import run_benchmark, write_partitioned
from src.config import SETTINGS
from pathlib import Path


def _new_run_id():
    return uuid.uuid4().hex[:12]


@click.group()
def cli():
    pass


@cli.command("validate-env")
def validate_env():
    import pandas, pyarrow, psycopg2, sqlalchemy  # noqa: F401
    click.echo(f"[validate-env] Python {sys.version.split()[0]} - core imports OK")
    click.echo(f"[validate-env] settings loaded: {list(SETTINGS['paths'].keys())}")


@cli.command("extract")
@click.option("--run-id", default=None)
def extract_cmd(run_id):
    run_id = run_id or _new_run_id()
    path = extract_sources(run_id)
    click.echo(f"[extract] run_id={run_id} -> {path}")


@cli.command("run-all")
@click.option("--run-id", default=None)
@click.option("--skip-extract", is_flag=True, default=False)
def run_all(run_id, skip_extract):
    run_id = run_id or _new_run_id()
    try:
        if not skip_extract:
            extract_sources(run_id)
        raw_dir = Path(SETTINGS["paths"]["raw_dir"]) / f"run_id={run_id}"
        stage_counts = run_staging(raw_dir, run_id)
        curated_counts = run_curated(run_id)
        click.echo(f"[run-all] run_id={run_id} staging={stage_counts} curated={curated_counts}")
    except Exception as exc:
        click.echo(f"[run-all] FAILED: {exc}", err=True)
        raise


@cli.command("load")
@click.option("--run-id", required=True)
def load_cmd(run_id):
    n = load_curated(run_id)
    click.echo(f"[load] run_id={run_id} upserted={n} rows")


@cli.command("validate")
@click.option("--run-id", required=True)
def validate_cmd(run_id):
    try:
        validate_curated(run_id)
    except ValidationError as exc:
        click.echo(str(exc), err=True)
        sys.exit(1)


@cli.command("benchmark")
@click.option("--run-id", required=True)
@click.option("--repeats", default=5)
def benchmark_cmd(run_id, repeats):
    out = run_benchmark(run_id, repeats=repeats)
    click.echo(f"[benchmark] results -> {out}")


@cli.command("partition")
@click.option("--run-id", required=True)
def partition_cmd(run_id):
    out = write_partitioned(run_id)
    click.echo(f"[partition] partitioned dataset -> {out}")


@cli.command("load-partition")
@click.option("--year", required=True, type=int)
@click.option("--month", required=True, type=int)
@click.option("--run-id", default=None)
def load_partition_cmd(year, month, run_id):
    run_id = run_id or _new_run_id()
    n = load_partition_(year, month, run_id)
    click.echo(f"[load-partition] year={year} month={month} run_id={run_id} rows={n}")


if __name__ == "__main__":
    cli()