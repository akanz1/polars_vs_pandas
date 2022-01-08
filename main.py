from src.pandas_utils import PandasUtils
from src.polars_utils import PolarsUtils


def pandas_code():
    pd_utils = PandasUtils()
    pd_utils.load_data()
    pd_utils.add_region_names_to_match()
    pd_utils.prepare_item_data()
    pd_utils.prepare_player_data()


def polars_code():
    pl_utils = PolarsUtils()
    pl_utils.load_data()
    pl_utils.add_region_names_to_match()
    pl_utils.prepare_item_data()
    pl_utils.prepare_player_data()

    # lazy scan csv


def main():
    # pandas_code()
    polars_code()


if __name__ == "__main__":
    # main()
    import pandas as pd

    # data = pd.DataFrame(
    #     {
    #         "names": ["foo", "ham", "spam", "cheese", "egg", "foo"],
    #         "dates": ["1", "1", "2", "3", "3", "4"],
    #         "groups": ["A", "A", "B", "B", "B", "C"],
    #     }
    # )
    # print(data)
    # data = data.groupby(["groups", "dates"])["names"].apply(list).reset_index()
    # print(data)
    # data = (
    #     data.groupby(["groups"])
    #     .apply(lambda x: dict(zip(x["dates"], x["names"])))
    #     .reset_index(name="combined")
    # )
    # print(data)
    # is there a way to do this like:
    # df.groupby(pl.to_dict("dates", "names"))
    import polars as pl

    data = pl.DataFrame(
        {
            "names": ["foo", "ham", "spam", "cheese", "egg", "foo"],
            "dates": ["1", "1", "2", "3", "3", "4"],
            "groups": ["A", "A", "B", "B", "B", "C"],
        }
    )
    print(data)
    data = data.groupby(["groups", "dates"])["names"].agg_list()
    print(data)
    data = (
        data.to_pandas()
        .groupby(["groups"])
        .apply(lambda x: dict(zip(x["dates"], x["names_agg_list"])))
        .reset_index(name="combined")
    )

    print(data)
    data = pl.from_pandas(data)
    print(data)

    # # def build_dict(args: list[pl.Series]) -> pl.Series:
    # #     import json

    # #     print(json.dumps(dict(zip(args[0].to_list(), args[1].to_list()))))
    # #     d = pl.read_json(
    # #         json.dumps(dict(zip(args[0].to_list(), args[1].to_list()))),
    # #     )
    # #     print(d)

    # #     return d.to_series(0)

    # # data = data.groupby(["groups"]).agg(
    # #     pl.apply(exprs=["dates", "names_agg_list"], f=build_dict).alias("combined")
    # # )

    # # build_dict([data["dates"], data["names_agg_list"]])

    # print(data)
