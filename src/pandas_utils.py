import pandas as pd

DATA_DIR = "data/dota"


class PandasUtils:
    def load_data(self):
        self.pd_match = pd.read_csv(f"{DATA_DIR}/match.csv")
        self.pd_cluster_regions = pd.read_csv(f"{DATA_DIR}/cluster_regions.csv")
        self.pd_purchase_log = pd.read_csv(f"{DATA_DIR}/purchase_log.csv").sample(
            frac=0.01
        )
        self.pd_item_id_names = pd.read_csv(f"{DATA_DIR}/item_ids.csv")
        self.pd_players = pd.read_csv(f"{DATA_DIR}/players.csv")

    def add_region_names_to_match(self):
        return pd.merge(
            self.pd_match, self.pd_cluster_regions, how="left", on="cluster"
        ).drop(columns="cluster")

    def prepare_item_data(self):
        data = pd.merge(
            self.pd_purchase_log, self.pd_item_id_names, how="left", on="item_id"
        ).drop(columns="item_id")

        data = (
            data.groupby(["match_id", "player_slot", "item_name"])["time"]
            .apply(list)
            .reset_index()
        )

        return (
            data.groupby(["match_id", "player_slot"])
            .apply(lambda x: dict(zip(x["item_name"], x["time"])))
            .reset_index(name="purchases")
        )

    def prepare_player_data(self):
        data = self.pd_players.query("account_id != 0")
        data = pd.merge(data, self.pd_purchase_log, on=["match_id", "player_slot"])

        pd_final = pd.merge(data, self.pd_match, how="left", on="match_id")
        pd_final = pd_final.dropna(
            axis=1, thresh=int(0.8 * len(pd_final))
        )  # at least 80% non nan
