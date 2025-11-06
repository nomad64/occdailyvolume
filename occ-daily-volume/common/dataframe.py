import pandas as pd
from tabulate import tabulate


def pretty_print_df(df_to_print: pd.DataFrame):
    """
    Print dataframe in a pretty table format

    :param df_to_print: dataframe to print
    :type df_to_print: pd.DataFrame
    """
    df_display = df_to_print.copy()
    df_display.index = df_display.index.strftime('%Y-%m-%d')
    df_display = df_display.reset_index()

    # Manually format numeric columns to strings with commas
    for col in df_display.select_dtypes(include='number').columns:
        df_display[col] = df_display[col].apply('{:,.0f}'.format)

    # Insert position number column with an empty name
    df_display.insert(0, '', range(1, len(df_display) + 1))

    print(
        tabulate(
            df_display,
            headers=df_display.columns,
            tablefmt="psql",
            numalign="right",
            stralign="right",
            showindex=False
        )
    )


if __name__ == "__main__":
    print("This file cannot be run directly.")
