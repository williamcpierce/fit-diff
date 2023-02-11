import pandas as pd


class FitDiff:
    def __init__(self, fit: dict, compare: dict) -> None:
        self.qty_diff = self._create_qty_diff(
            self._parse_input_file(fit["filepath"], fit["input_type"]),
            self._parse_input_file(compare["filepath"], compare["input_type"]),
        )
        self.summary_tables = self._create_summary_tables(self.qty_diff)

    def __repr__(self) -> str:
        tables = [
            f"\n{name}:\n{table.to_string()}\n"
            for name, table in self.summary_tables.items()
        ]
        return "".join(tables)

    def _create_qty_diff(
        self, fit_data: pd.DataFrame, compare_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Compare the quantity data in the input DataFrames by item. The method performs the
        following steps:
        - Aggregate the quantity data by item using _aggregate_item_qty method.
        - Merge the aggregated data from the two input DataFrames using an outer join.
        - Fill missing values with 0.
        - Calculate the difference between the fit and compare quantities.
        Return a new DataFrame with columns "item", "qty_fit", "qty_compare", and
        "qty_diff", sorted by "item".
        """
        for df in [fit_data, compare_data]:
            if not all(col in df.columns for col in ["item", "qty"]):
                raise ValueError(f"DataFrame must have columns 'item' and 'qty'")
        df = (
            pd.merge(
                self._aggregate_item_qty(fit_data),
                self._aggregate_item_qty(compare_data),
                on="item",
                how="outer",
                suffixes=["_fit", "_compare"],
            )
            .fillna(0)
            .sort_values("item")
        )
        df[["qty_fit", "qty_compare"]] = df[["qty_fit", "qty_compare"]].astype(int)
        df["qty_diff"] = df["qty_fit"] - df["qty_compare"]
        return df

    @staticmethod
    def _parse_input_file(filepath: str, input_format: str) -> pd.DataFrame:
        """
        Given a file path and an input format ("contents" or "multibuy"), parse the file
        and return a DataFrame with columns "item" and "qty".
        """
        if input_format == "contents":
            parsed_data = pd.read_table(
                filepath,
                header=None,
                names=["item", "type", "location", "qty"],
                usecols=["item", "qty"],
            )
        elif input_format == "multibuy":
            parsed_data = pd.read_table(
                filepath,
                header=None,
                sep=r"[\s][x](?=\d)",
                names=["item", "qty"],
                engine="python",
            ).fillna(1)
        elif input_format == "eft":
            parsed_data = pd.read_table(
                filepath,
                header=None,
                sep=r"[\s][x](?=\d)",
                names=["item", "qty"],
                engine="python",
            ).fillna(1)
            # parsed_data = parsed_data[~parsed_data["item"].str.startswith("[")]
            parsed_data["item"] = parsed_data["item"].str.strip("[]")
            parsed_data["item"] = parsed_data["item"].str.split(",").str[0]
        else:
            raise ValueError(f"Unrecognized input format: {input_format}")

        return parsed_data

    @staticmethod
    def _aggregate_item_qty(data: pd.DataFrame) -> pd.DataFrame:
        """
        Given a DataFrame with columns "item" and "qty", group the rows by "item" and
        sum the "qty" values for each group.
        Return a new DataFrame with columns "item" and "qty", sorted by "item".
        """
        return (
            data.groupby("item", as_index=False).agg({"qty": "sum"}).sort_values("item")
        )

    @staticmethod
    def _create_summary_tables(qty_diff: pd.DataFrame) -> dict:
        """
        Create summary tables from a given DataFrame with columns "item", "qty_fit", "qty_compare", and
        "qty_diff".
        Return a dict of three new DataFrames:
        - Correct Items: Items with zero difference in quantity.
        - Missing Items: Items with negative difference in quantity (i.e., in fit_data but not in compare_data).
        - Extra Items: Items with positive difference in quantity (i.e., in compare_data but not in fit_data).
        """
        correct_items = qty_diff[qty_diff["qty_diff"] == 0]
        missing_items = qty_diff[qty_diff["qty_diff"] < 0]
        extra_items = qty_diff[qty_diff["qty_diff"] > 0]
        return {
            "Correct Items": correct_items,
            "Missing Items": missing_items,
            "Extra Items": extra_items,
        }


if __name__ == "__main__":
    fit_diff = FitDiff(
        compare={"filepath": "file_formats/eft.txt", "input_type": "eft"},
        fit={
            "filepath": "file_formats/multibuy.txt",
            "input_type": "multibuy",
        },
    )
    print(fit_diff)
