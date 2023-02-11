import pandas as pd


class FitDiff:
    def __init__(self, user: str, doctrine: str) -> None:
        self.user = self._parse_input_file(
            filepath=user,
            type="multibuy",
        )
        self.doctrine = self._parse_input_file(
            filepath=doctrine,
            type="contents",
        )
        self.quantity_diff = self._create_quantity_diff()
        self.summary_tables = self._create_summary_tables()

    def __str__(self) -> str:
        sep = "=" * 60
        lines = [
            f"{sep}\n{name}:\n{table.to_string()}\n"
            for name, table in self.summary_tables.items()
        ]
        return "".join(lines)

    def _parse_input_file(
        self,
        filepath: str,
        type: str,
        item_col: str = "item",
        quantity_col: str = "quantity",
    ) -> pd.DataFrame:
        try:
            if type == "contents":
                df = pd.read_table(
                    filepath,
                    header=None,
                    names=[item_col, "type", "location", quantity_col],
                    usecols=[item_col, quantity_col],
                    dtype={quantity_col: "int"},
                )
            elif type == "multibuy":
                df = pd.read_table(
                    filepath,
                    header=None,
                    sep=r"[\s][x](?=\d)",
                    names=[item_col, quantity_col],
                    engine="python",
                ).fillna(1)
            else:
                pass
            return (
                df.groupby(item_col, as_index=False)
                .agg({quantity_col: "sum"})
                .sort_values(item_col)
            )
        except (
            FileNotFoundError,
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
        ) as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    def _create_quantity_diff(self) -> pd.DataFrame:
        try:
            df = pd.merge(
                self.user,
                self.doctrine,
                on="item",
                how="outer",
                suffixes=["_user", "_doctrine"],
            ).fillna(0)
            df[["quantity_user", "quantity_doctrine"]] = df[
                ["quantity_user", "quantity_doctrine"]
            ].astype(int)
            df = df.assign(
                quantity_diff=df["quantity_user"] - df["quantity_doctrine"]
            ).sort_values("item")
            return df
        except (KeyError) as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    def _create_summary_tables(self) -> dict:
        try:
            correct_items = self.quantity_diff.query("quantity_diff == 0").reset_index(
                drop=True
            )
            missing_items = self.quantity_diff[
                self.quantity_diff["quantity_diff"] < 0
            ].reset_index(drop=True)
            extra_items = self.quantity_diff[
                self.quantity_diff["quantity_diff"] > 0
            ].reset_index(drop=True)
            return {
                "Correct Items": correct_items,
                "Missing Items": missing_items,
                "Extra Items": extra_items,
            }
        except (
            KeyError,
            pd.errors.UndefinedVariableError,
        ) as e:
            print(f"Error: {e}")
            return pd.DataFrame()


diff = FitDiff(
    user="examples/multibuy.txt",
    doctrine="examples/contents.tsv",
)
print(str(diff))
