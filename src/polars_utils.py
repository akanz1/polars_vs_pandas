import polars as pl

DATA_DIR = "data/dota"


class PolarsUtils:
    def load_data(self):
        self.pl_match = pl.read_csv(f"{DATA_DIR}/match.csv")
        self.pl_cluster_regions = pl.read_csv(f"{DATA_DIR}/cluster_regions.csv")
        self.pl_purchase_log = pl.read_csv(f"{DATA_DIR}/purchase_log.csv")
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

        return (
            data.groupby(["match_id", "player_slot", "item_name"])
            .agg(pl.col("time").list().keep_name())
            .groupby(["match_id", "player_slot"])
            .agg(
                [
                    pl.apply(
                        [pl.col("item_name"), pl.col("time")],
                        lambda s: dict(zip(s[0], s[1].to_list())),
                    ).alias("purchases")
                ]
            )
        )

    def prepare_player_data(self, matches, purchases):
        pl_final = (
            self.pl_players.filter(pl.col("account_id") != 0)
            .join(purchases, on=["match_id", "player_slot"])
            .join(matches, how="left", on="match_id")
        )

        pl_final = pl_final[
            :,
            [
                pl_final[col].null_count() <= 0.2 * pl_final.height
                for col in pl_final.columns
                if col not in ["purchases"]
            ]
            + ["purchases"],
        ]  # drop cols with more than 20% NaN
        return pl_final
