# Academy zu Polars

## Polars

- Written in Rust
- Relies on Arrow for Memory Mapping and column oriented data access
- Offers a Python wrapper
- Multi-threaded comapred to single-threaded numpy and pandas
- (Semi-) Lazy execution
- Query optimizations (e.g. predicate pushdown, projection pushdown, …)
- Operations run in parallelizable contexts -> each column operation runs in parallel
- Proper NaN dtypes (unlike pandas where pd.NA is float)
- ...

## Daten

Dota 2 [Datensatz](https://www.kaggle.com/devinanzelmo/dota-2-matches) von Kaggle im format `.csv`

### Verwendete Tabellen

- `match.csv` (2.6MB) - Informationen zu den gespielten Spielen (50K Spiele)

```python
       match_id  start_time  duration  tower_status_radiant   ...
0             0  1446750112      2375                  1982   ...
1             1  1446753078      2582                     0   ...
2             2  1446764586      2716                   256   ...
3             3  1446765723      3085                     4   ...
```

- `cluster_regions.csv` (1KB) - Informationen zu den Clustern (geographische Regionen)

```python
    cluster                region
0       111               US WEST
1       112               US WEST
2       113               US WEST
3       121               US EAST
```

- `purchase_log.csv` (289.8MB) - Informationen zu den gekauften Items (~18Mio Transaktionen)

```python
┌─────────┬──────┬─────────────┬──────────┐
│ item_id ┆ time ┆ player_slot ┆ match_id │
│ ---     ┆ ---  ┆ ---         ┆ ---      │
│ i64     ┆ i64  ┆ i64         ┆ i64      │
╞═════════╪══════╪═════════════╪══════════╡
│ 44      ┆ -81  ┆ 0           ┆ 0        │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ 29      ┆ -63  ┆ 0           ┆ 0        │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ 43      ┆ 6    ┆ 0           ┆ 0        │
└─────────┴──────┴─────────────┴──────────┘
```

- `item_ids.csv` (3KB) - Informationen zu den Item Namen

```python
     item_id         item_name
0          1             blink
1          2  blades_of_attack
2          3        broadsword
3          4         chainmail
```

- `players.csv` (126.9MB) - Informationen zu den Spielern (500K Einträge, 10 pro Spiel)

```python
        match_id  account_id  hero_id  player_slot  gold  gold_spent   ...
0              0           0       86            0  3261       10960   ...
1              0           1       51            1  2954       17760   ...
2              0           0       83            2   110       12195   ...
3              0           2       11            3  1179       22505   ...
```

*Anonymous users have the value of 0 for account_id*
