import pandas as pd


class FitDiff:
    def __init__(self, user: str, doctrine: str) -> None:
        self.user = self._parse_multibuy_file(user)
        self.doctrine = self._parse_contents_file(doctrine)
        self.diff = self._create_quantity_diff()
        self.summary_tables = self._create_summary_tables()

    def __str__(self) -> str:
        sep = "=" * 60
        returnstr = ""
        for name, table in self.summary_tables.items():
            returnstr += f"{sep}\n{name}:\n{table.to_string(index=False)}\n"

        return returnstr

    @staticmethod
    def _parse_contents_file(filepath: str) -> pd.DataFrame:
        contents = pd.read_csv(
            filepath,
            header=None,
            names=["item", "type", "location", "quantity"],
            usecols=["item", "quantity"],
            delimiter="\t",
        )
        contents["item"] = contents["item"].str.strip()
        contents["quantity"] = contents["quantity"].astype(int)
        contents = (
            contents.groupby("item")["quantity"].sum().reset_index(name="quantity")
        )
        return contents.sort_values("item")

    @staticmethod
    def _parse_multibuy_file(filepath: str) -> pd.DataFrame:
        return (
            pd.read_csv(filepath, header=None)[0]
            .str.split(r"([\s])[x](?=\d)", expand=True)
            .drop(1, axis="columns")
            .rename(columns={0: "item", 2: "quantity"})
            .fillna(0)
            .groupby("item")["quantity"]
            .sum()
            .reset_index(name="quantity")
            .sort_values("item")
        )

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
        diff = joined.assign(diff=joined["quantity_user"] - joined["quantity_doctrine"])
        return diff.sort_values("item")

    def _create_summary_tables(self) -> dict:
        correct_items = self.diff.query("diff == 0")[
            ["item", "quantity_doctrine"]
        ].reset_index(drop=True)
        correct_items = correct_items.rename(columns={"quantity_doctrine": "quantity"})

        def create_summary(df: pd.DataFrame, operator: str) -> pd.DataFrame:
            summary = df.query(f"diff {operator} 0")[["item", "diff"]].reset_index(
                drop=True
            )
            summary = summary.rename(columns={"diff": "quantity"})
            summary["quantity"] = summary["quantity"].abs()
            return summary

        missing_items = create_summary(self.diff, "<")
        extra_items = create_summary(self.diff, ">")

        return {
            "Correct Items": correct_items,
            "Missing Items": missing_items,
            "Extra Items": extra_items,
        }


diff = FitDiff(
    user="examples/multibuy.txt",
    doctrine="examples/contents.tsv",
)
print(str(diff))
