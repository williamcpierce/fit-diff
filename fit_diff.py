import pandas as pd
import time


class FitDiff:
    def __init__(self, fit: dict, doctrine: dict) -> None:
        self.qty_diff = self._create_qty_diff(
            self._parse_input_file(fit),
            self._parse_input_file(doctrine),
        )
        self.summary_tables = self.qty_diff.pipe(self._create_summary_tables)

    def __repr__(self) -> str:
        tables = [
            f"\n{name}:\n{table.to_string(index=False)}\n"
            for name, table in self.summary_tables.items()
        ]
        return "".join(tables)

    def _create_qty_diff(
        self,
        fit_data: pd.DataFrame,
        doctrine_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        doctrine the quantity data in the input DataFrames by item. The method performs the
        following steps:
        - Aggregate the quantity data by item using _aggregate_item_qty method.
        - Merge the aggregated data from the two input DataFrames using an outer join.
        - Fill missing values with 0.
        - Calculate the difference between the fit and doctrine quantities.
        Return a new DataFrame with columns "item", "qty_fit", "qty_doctrine", and
        "qty_diff", sorted by "item".
        """
        return (
            pd.merge(
                fit_data.pipe(self._aggregate_item_qty),
                doctrine_data.pipe(self._aggregate_item_qty),
                on="item",
                how="outer",
                suffixes=["_fit", "_doctrine"],
            )
            .fillna(0)
            .astype({"qty_fit": "int", "qty_doctrine": "int"})
            .sort_values("item")
            .assign(qty_diff=lambda x: x["qty_fit"] - x["qty_doctrine"])
        )

    @staticmethod
    def _parse_input_file(input_: dict) -> pd.DataFrame:
        """
        Given a dict with values for a file path and an input format ("contents" or "multibuy"),
        parse the file and return a DataFrame with columns "item" and "qty".
        """
        match input_["format"]:
            case "contents":
                return pd.read_table(
                    input_["filepath"],
                    header=None,
                    names=["item", "type", "location", "qty"],
                    usecols=["item", "qty"],
                )
            case "multibuy":
                return pd.read_table(
                    input_["filepath"],
                    header=None,
                    sep=r"[\s][x](?=\d)",
                    names=["item", "qty"],
                    engine="python",
                ).fillna(1)
            case "eft":
                return (
                    pd.read_table(
                        input_["filepath"],
                        header=None,
                        sep=r"[\s][x](?=\d)",
                        names=["item", "qty"],
                        engine="python",
                    )
                    .fillna(1)
                    .assign(
                        item=lambda x: x["item"].str.strip("[]").str.split(",").str[0]
                    )
                )
            case _:
                raise ValueError(f"Unrecognized input format: {input_['format']}")

    @staticmethod
    def _aggregate_item_qty(df: pd.DataFrame) -> pd.DataFrame:
        """
        Given a DataFrame with columns "item" and "qty", group the rows by "item" and
        sum the "qty" values for each group.
        Return a new DataFrame with columns "item" and "qty", sorted by "item".
        """
        return (
            df.groupby("item", as_index=False).agg({"qty": "sum"}).sort_values("item")
        )

    @staticmethod
    def _create_summary_tables(df: pd.DataFrame) -> dict:
        """
        Create summary tables from a given DataFrame with columns "item", "qty_fit", "qty_doctrine", and
        "qty_diff".
        Return a dict of three new DataFrames:
        - Correct Items: Items with zero difference in quantity.
        - Missing Items: Items with negative difference in quantity (i.e., in fit_data but not in doctrine_data).
        - Extra Items: Items with positive difference in quantity (i.e., in doctrine_data but not in fit_data).
        """
        return {
            "Correct Items": df[df["qty_diff"] == 0],
            "Missing Items": df[df["qty_diff"] < 0],
            "Extra Items": df[df["qty_diff"] > 0],
        }


if __name__ == "__main__":
    start_time = time.time()
    fit_diff = FitDiff(
        fit={"filepath": "file_formats/multibuy.txt", "format": "multibuy"},
        doctrine={"filepath": "file_formats/eft.txt", "format": "eft"},
        # doctrine={"filepath": "file_formats/contents.tsv", "format": "contents"},
    )
    print(fit_diff)
    print("--- %s ms ---" % round((time.time() - start_time) * 1000, 2))
