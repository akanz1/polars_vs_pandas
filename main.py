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
    main()
