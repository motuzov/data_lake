import pytest
from datetime import timedelta, datetime, date
import polars as pl
from f_res.eng import chunk_dates_generator


@pytest.fixture
def to_agg():
    pf = (
        pl.DataFrame(
            {
                "date": pl.datetime_range(
                    datetime(2026, 3, 1), datetime(2026, 3, 5), "6h", eager=True
                )
            }
        )
        .join(other=pl.DataFrame({"uid": pl.int_range(0, 2, eager=True)}), how="cross")
        .with_columns((pl.col("date").dt.hour() % 12).alias("h"))
        .with_columns(pl.lit(1).alias("v"))
        .with_columns(pl.col("date").dt.date())
    )
    pf.group_by(["uid", "date", "h"]).agg(pl.col("v").sum()).pivot(
        index=["uid", "date"], on=["h"]
    )


def test_chunk_dates_generator():
    expected_ranges = [
        ("2026-02-01", "2026-02-08"),
        ("2026-02-09", "2026-02-16"),
        ("2026-02-17", "2026-02-24"),
        ("2026-02-25", "2026-03-02"),
    ]
    for i, (start, end) in enumerate(
        chunk_dates_generator(start=date(2026, 2, 1), end=date(2026, 3, 1), step_days=7)
    ):
        assert expected_ranges[i] == (str(start), str(end))
