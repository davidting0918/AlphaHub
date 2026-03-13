"""
Job Manager — Unified CLI entry point for all pipeline jobs.

Usage:
    python3 -m pipeline.job_manager --name OKX_MAIN_01 instrument
    python3 -m pipeline.job_manager --name OKX_MAIN_01 --start 20260101 --end 20260301 funding_rate
"""

import asyncio
import argparse
import logging
from datetime import datetime, timezone

from pipeline.jobs import InstrumentJob, FundingRateJob, KlineJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

JOB_REGISTRY = {
    "instrument": InstrumentJob,
    "funding_rate": FundingRateJob,
    "kline": KlineJob,
}


def parse_date(s: str) -> datetime:
    """Parse YYYYMMDD or YYYY-MM-DD to UTC datetime."""
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s}. Use YYYYMMDD or YYYY-MM-DD.")


async def async_main():
    parser = argparse.ArgumentParser(
        description="AlphaHub Pipeline Job Manager",
        usage="python3 -m pipeline.job_manager --name PORTFOLIO_NAME [--start DATE] [--end DATE] JOB_TYPE",
    )

    parser.add_argument("--name", required=True, help="Portfolio name (e.g. OKX_MAIN_01)")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYYMMDD or YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYYMMDD or YYYY-MM-DD)")
    parser.add_argument("job_type", choices=list(JOB_REGISTRY.keys()), help="Job type to run")

    args = parser.parse_args()

    start = parse_date(args.start) if args.start else None
    end = parse_date(args.end) if args.end else None

    job_class = JOB_REGISTRY[args.job_type]
    job = job_class(portfolio_name=args.name, start=start, end=end)

    logger.info(
        f"Running {args.job_type} | portfolio={args.name}"
        + (f" | start={start}" if start else "")
        + (f" | end={end}" if end else "")
    )

    await job.execute()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
