import pytest
from datetime import timedelta, datetime, date
import polars as pl


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
