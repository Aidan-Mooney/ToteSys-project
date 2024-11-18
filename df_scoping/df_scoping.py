import pandas as pd
from os import listdir


def make_dfs(dir, extension=".parquet"):
    parquet_filenames = listdir(dir)
    dataframes = {}
    for filename in parquet_filenames:
        dataframes[filename[: -len(extension)]] = pd.read_parquet(f"{dir}{filename}")
    return dataframes


if __name__ == "__main__":
    dataframes = make_dfs("df_scoping/tables/")
    print(dataframes)
