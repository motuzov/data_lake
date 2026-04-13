from typing import Callable, Generator

from datetime import timedelta, datetime, date
from abc import abstractmethod
import polars as pl


def chunk_dates_generator(
    start: date, end: date, step_days: float = 3
) -> Generator[tuple[date, date], None, None]:
    current = start
    # end = end + timedelta(days=1)
    step_d: timedelta = timedelta(1.0 if step_days == 0 else step_days)
    while current <= end:
        right = current + step_d
        right = right if right <= end else end
        yield (current, right)
        current += step_d + timedelta(days=1)


days = float
AssetCallback = Callable[[pl.LazyFrame], pl.LazyFrame]


class DateRangeChunksIOManager:
    def __init__(self, storage_options: dict = {}) -> None:
        self.storage_options: dict = storage_options
        self.in_columns: list[str] = []
        self.date_col = ""
        self.input_tb_path: str = ""
        self.output_tb_path: str = ""

    @abstractmethod
    def read_chunk(self, start: date, end: date) -> pl.LazyFrame:
        raise NotImplementedError("DateRangeChunksIOManager must implement read_chunk")

    @abstractmethod
    def write_chunk(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("DateRangeChunksIOManager must implement write_chunk")

    def setup(
        self,
        input_tb_path: str,
        output_tb_path: str,
        date_column: str,
        in_columns: list[str],
    ):
        self.input_tb_path = input_tb_path
        self.output_tb_path = output_tb_path
        self.date_column = date_column
        self.in_columns = in_columns

    def chunk_dates_generator(
        self, start: date, end: date, step_days: float = 3
    ) -> Generator[tuple[date, date], None, None]:
        current = start
        end = end + timedelta(days=1)
        step_d: timedelta = timedelta(1.0 if step_days == 0 else step_days)
        while current < end:
            right = current + step_d
            right = right if right <= end else end
            yield (current, right)
            current += step_d + timedelta(days=1)


class FakeDRCIOManager(DateRangeChunksIOManager):
    def __init__(self) -> None:
        self.result: list[pl.DataFrame] = []
        pass

    def read_chunk(self, start: date, end: date) -> pl.LazyFrame:
        return pl.DataFrame({"start": start, "end": end}).lazy()

    def write_chunk(self, df: pl.DataFrame) -> None:
        self.result.append(df)


class RawSrcDRCIOManager(DateRangeChunksIOManager):
    def __init__(self, storage_options: dict) -> None:
        super().__init__(storage_options=storage_options)

    def read_chunk(self, start: date, end: date) -> pl.LazyFrame:
        return (
            pl.scan_delta(self.imput_tb_path, storage_options=self.storage_options)
            .select(self.in_columns)
            .filter(
                pl.col(self.date_column).is_in(
                    [str(d) for d in pl.date_range(start, end, eager=True)]
                )
            )
        )

    def write_chunk(self, pf: pl.DataFrame) -> None:
        pass


def get_expr_columns(exprs: list[pl.Expr]) -> list[str]:
    return [expr.meta.root_names() for expr in exprs]


class DailyAggEngine:
    def __init__(
        self, chunks_io_manager: DateRangeChunksIOManager = DateRangeChunksIOManager()
    ):
        self.chunks_io_manager = chunks_io_manager
        self.by: list[str] | None = None
        self.aggs: list[pl.Expr] | None = None
        self.on_start: AssetCallback | None = (None,)
        self.on_end: AssetCallback | None = (None,)

    def _group_by_agg_chunk(
        self,
        lf: pl.LazyFrame,
    ) -> pl.LazyFrame:
        if self.on_start:
            lf = self.on_start(lf)
        if self.aggs and self.by:
            lf = lf.group_by(by=self.by).agg(self.aggs)
        if self.on_end:
            lf = self.on_end(lf)
        return lf

    def group_by_agg(
        self,
        input_tb_path: str,
        output_tb_path: str,
        start_date: str,
        end_date: str,
        chunk_size: days,
        date_column: str,
        index: str,
        extra_by_columns: list[str] = [],
        aggs: list[pl.Expr] | None = None,
        on_group_by_agg_chunk_start: AssetCallback | None = None,
        on_group_by_agg_chunk_end: AssetCallback | None = None,
    ):
        by = [index, date_column] + extra_by_columns
        agg_columns = get_expr_columns(aggs) if aggs else []
        self.chunks_io_manager.setup(
            input_tb_path=input_tb_path,
            output_tb_path=output_tb_path,
            in_columns=by + [date_column] + agg_columns,
            date_col=date_column,
        )

        self.by = by
        self.aggs = aggs
        self.on_start = on_group_by_agg_chunk_start
        self.on_end = on_group_by_agg_chunk_end
        for (
            chunk_date_start,
            chunk_date_end,
        ) in self.chunks_io_manager.chunk_dates_generator(
            start=start_date, end=end_date, step_days=chunk_size
        ):
            lf = self._group_by_agg_chunk(
                lf=self.chunks_io_manager.read_chunk(
                    start=chunk_date_start, end=chunk_date_end
                ),
                by=by,
                aggs=aggs,
                on_start=on_group_by_agg_chunk_start,
                on_end=on_group_by_agg_chunk_end,
            )
            lf = lf.pivot(index=[index, date_column], on=extra_by_columns)
            self.chunks_io_manager.write_chunk(df=lf.collect())
