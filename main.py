import pandas as pd


class FitDiff:
    def __init__(self, user: str, doctrine: str) -> None:
        self.user = self._parse_multibuy_file(user)
        self.doctrine = self._parse_contents_file(doctrine)
        self.quantity_diff = self._create_quantity_diff()
        self.summary_tables = self._create_summary_tables()

    def __str__(self) -> str:
        sep = "=" * 60
        lines = [
            f"{sep}\n{name}:\n{table.to_string()}\n"
            for name, table in self.summary_tables.items()
        ]
        return "".join(lines)

    @staticmethod
    def _parse_contents_file(
        filepath: str,
        item_col: str = "item",
        quantity_col: str = "quantity",
    ) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                filepath,
                header=None,
                names=[item_col, "type", "location", quantity_col],
                usecols=[item_col, quantity_col],
                delimiter="\t",
            )

            df[item_col] = df[item_col].str.strip()
            df[quantity_col] = df[quantity_col].astype(int)
            df = (
                df.groupby(item_col, as_index=False)
                .agg({"quantity": "sum"})
                .sort_values("item")
            )
            return df
        except (
            FileNotFoundError,
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
        ) as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    @staticmethod
    def _parse_multibuy_file(filepath: str) -> pd.DataFrame:
        try:
            df = (
                pd.read_csv(
                    filepath,
                    header=None,
                    sep=r"[\s][x](?=\d)",
                    names=["item", "quantity"],
                    engine="python",
                )
                .fillna(1)
                .groupby("item", as_index=False)
                .agg({"quantity": "sum"})
                .sort_values("item")
            )
            return df
        except (
            FileNotFoundError,
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
        ) as e:
            print(f"Error: {e}")
            return pd.DataFrame()

    def _create_quantity_diff(self) -> pd.DataFrame:
        joined = pd.merge(
            self.user,
            self.doctrine,
            on="item",
            how="outer",
            suffixes=["_user", "_doctrine"],
        ).fillna(0)
        joined[["quantity_user", "quantity_doctrine"]] = joined[
            ["quantity_user", "quantity_doctrine"]
        ].astype(int)
        quantity_diff = joined.assign(
            quantity_diff=joined["quantity_user"] - joined["quantity_doctrine"]
        )
        return quantity_diff.sort_values("item")

    def _create_summary_tables(self) -> dict:
        correct_items = self._create_correct_items_table()
        missing_items = self._create_missing_items_table()
        extra_items = self._create_extra_items_table()
        return {
            "Correct Items": correct_items,
            "Missing Items": missing_items,
            "Extra Items": extra_items,
        }

    def _create_correct_items_table(self) -> pd.DataFrame:
        correct_items = self.quantity_diff.query("quantity_diff == 0")[
            ["item", "quantity_doctrine"]
        ].reset_index(drop=True)
        correct_items = correct_items.rename(columns={"quantity_doctrine": "quantity"})
        return correct_items

    def _create_missing_items_table(self) -> pd.DataFrame:
        missing_items = self._create_summary(self.quantity_diff["quantity_diff"] < 0)
        return missing_items

    def _create_extra_items_table(self) -> pd.DataFrame:
        extra_items = self._create_summary(self.quantity_diff["quantity_diff"] > 0)
        return extra_items

    def _create_summary(self, mask: pd.Series) -> pd.DataFrame:
        summary = self.quantity_diff[mask][["item", "quantity_diff"]].reset_index(
            drop=True
        )
        summary = summary.rename(columns={"quantity_diff": "quantity"})
        summary["quantity"] = summary["quantity"].abs()
        return summary


diff = FitDiff(
    user="examples/multibuy.txt",
    doctrine="examples/contents.tsv",
)
print(str(diff))
