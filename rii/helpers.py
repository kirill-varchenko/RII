from datetime import date, datetime
from typing import Optional, Union

import pandas as pd


def parse_date_to_week(value: Union[str, date, datetime]) -> str:
    try:
        if isinstance(value, str):
            d = date.fromisoformat(value)
        elif isinstance(value, datetime):
            d = value.date()
        elif isinstance(value, date):
            d = value
        else:
            raise TypeError(value)

        return "{0.year}-W{0.week:02}".format(d.isocalendar())
    except:
        return ""


def count_frequency(
    df: pd.DataFrame, column: str, groupby: Optional[list[str]] = None
) -> pd.DataFrame:
    """Calculate grouped or not counts, frequencies and cum frequencies."""

    groupby = groupby or []

    base_name = " ".join(groupby)
    total_column_name = (base_name + " Total").lstrip()
    base_column_name = (base_name + " " + column).lstrip()
    count_column_name = (base_column_name + " Count").lstrip()
    frequency_column_name = (base_column_name + " Frequency").lstrip()
    cumfreq_column_name = (base_column_name + " CumFrequency").lstrip()

    if groupby:
        group_totals = (
            df.groupby(groupby).size().to_frame(total_column_name).reset_index()
        )

        group_column_counts = (
            df.groupby(groupby + [column])
            .size()
            .to_frame(count_column_name)
            .reset_index()
        )

        result_df = group_totals.merge(group_column_counts, on=groupby, how="left")
    else:
        result_df = df.groupby(column).size().to_frame(count_column_name).reset_index()
        result_df[total_column_name] = len(df)

    result_df[frequency_column_name] = (
        result_df[count_column_name] / result_df[total_column_name]
    )

    result_df.sort_values(
        groupby + [frequency_column_name],
        ascending=[True] * len(groupby) + [False],
        inplace=True,
    )

    if groupby:
        result_df[cumfreq_column_name] = result_df.groupby(groupby)[
            frequency_column_name
        ].cumsum()
    else:
        result_df[cumfreq_column_name] = result_df[frequency_column_name].cumsum()

    return result_df
