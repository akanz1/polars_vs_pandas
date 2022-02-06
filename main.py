from src.pandas_utils import PandasUtils
from src.polars_utils import PolarsUtils


def pandas_code():
    pd_utils = PandasUtils()
    pd_utils.load_data()
    pd_matches_with_regions = pd_utils.add_region_names_to_match()
    pd_purchases_with_names = pd_utils.prepare_item_data()
    pd_final = pd_utils.prepare_player_data(
        pd_matches_with_regions, pd_purchases_with_names
    )
    print(pd_final.head())


def polars_code():
    pl_utils = PolarsUtils()
    pl_utils.load_data()
    pl_matches_with_regions = pl_utils.add_region_names_to_match()
    pl_purchases_with_names = pl_utils.prepare_item_data()
    pl_final = pl_utils.prepare_player_data(
        pl_matches_with_regions, pl_purchases_with_names
    )
    print(pl_final.head())


# lazy scan csv


def main():
    # pandas_code()
    polars_code()


if __name__ == "__main__":
    main()
