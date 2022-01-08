import polars as pl

DATA_DIR = "data/dota"


class PolarsUtils:
    def load_data(self):
        self.pl_match = pl.read_csv(f"{DATA_DIR}/match.csv")
        self.pl_cluster_regions = pl.read_csv(f"{DATA_DIR}/cluster_regions.csv")
        self.pl_purchase_log = pl.read_csv(f"{DATA_DIR}/purchase_log.csv").sample(
            frac=0.2
        )
        self.pl_item_id_names = pl.read_csv(f"{DATA_DIR}/item_ids.csv")
        self.pl_players = pl.read_csv(f"{DATA_DIR}/players.csv")

    def add_region_names_to_match(self):
        return self.pl_match.join(
            self.pl_cluster_regions, how="left", on="cluster"
        ).drop("cluster")

    def prepare_item_data(self):
        data = self.pl_purchase_log.join(
            self.pl_item_id_names, how="left", on="item_id"
        ).drop("item_id")

        data = (
            data.groupby(["match_id", "player_slot", "item_name"])["time"]
            .agg_list()
            .with_column_renamed("time_agg_list", "time")
        )

        print(
            data.to_pandas()
            .groupby(["match_id", "player_slot"])
            .apply(lambda x: dict(zip(x["item_name"], x["time"])))
            .reset_index(name="purchases")
        )

    def prepare_player_data(self):
        data = self.pl_players.query("account_id != 0")
        data = data.join(self.pl_purchase_log, on=["match_id", "player_slot"])

        pl_final = data.join(self.pl_match, how="left", on="match_id")
        pl_final[:, [s.null_count() < 0.2 * pl_final.height for s in pl_final]]
        print(pl_final)
