"""
Job Manager — Unified CLI entry point for all pipeline jobs.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 instrument
    python3 -m pipeline.job_manager --name OKX_MAIN_01 --start 20260101 --end 20260301 funding_rate
"""

import argparse
import logging
from datetime import datetime, timezone

from pipeline.jobs import InstrumentJob, FundingRateJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Registry: job_type_name → Job class
JOB_REGISTRY = {
    "instrument": InstrumentJob,
    "funding_rate": FundingRateJob,
}


def parse_date(s: str) -> datetime:
    """Parse YYYYMMDD or YYYY-MM-DD to UTC datetime"""
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s}. Use YYYYMMDD or YYYY-MM-DD.")


def main():
    parser = argparse.ArgumentParser(
        description="AlphaHub Pipeline Job Manager",
        usage="python3 -m pipeline.job_manager --name PORTFOLIO_NAME [--start DATE] [--end DATE] JOB_TYPE",
    )

    parser.add_argument(
        "--name",
        required=True,
        help="Portfolio name (e.g. OKX_MAIN_01)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYYMMDD or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYYMMDD or YYYY-MM-DD)",
    )
    parser.add_argument(
        "job_type",
        choices=list(JOB_REGISTRY.keys()),
        help=f"Job type to run: {', '.join(JOB_REGISTRY.keys())}",
    )

    args = parser.parse_args()

    # Parse dates
    start = parse_date(args.start) if args.start else None
    end = parse_date(args.end) if args.end else None

    # Get job class
    job_class = JOB_REGISTRY[args.job_type]

    # Create and execute
    job = job_class(
        portfolio_name=args.name,
        start=start,
        end=end,
    )

    logger.info(
        f"Running {args.job_type} | portfolio={args.name}"
        + (f" | start={start}" if start else "")
        + (f" | end={end}" if end else "")
    )

    job.execute()


if __name__ == "__main__":
    main()
