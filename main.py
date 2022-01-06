from src.pandas_utils import PandasUtils


def pandas_code():
    pd_utils = PandasUtils()
    pd_utils.load_data()
    pd_utils.add_region_names_to_match()
    pd_utils.prepare_item_data()
    pd_utils.prepare_player_data()


def polars_code():
    pass
    # read csv
    # pl.read_csv()

    # lazy scan csv


def main():
    pandas_code()
    # polars_code()


if __name__ == "__main__":
    main()
