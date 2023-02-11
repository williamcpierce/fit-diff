import pandas as pd


class FitDiff:
    def __init__(self, user: str, doctrine: str) -> None:
        self.user = self._parse_multibuy(user)
        self.doctrine = self._parse_contents(doctrine)
        self.diff = self._diff_quantity()
        self.summary_tables = self._create_summary_tables()

    def __str__(self) -> str:
        sep = "=" * 60
        returnstr = ""
        for name, table in self.summary_tables.items():
            returnstr += f"{sep}\n{name}:\n{table.to_string(index=False)}\n"

        return returnstr

    @staticmethod
    def _parse_contents(filepath: str) -> pd.DataFrame:
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
    def _parse_multibuy(filepath: str) -> pd.DataFrame:
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

    def _diff_quantity(self) -> pd.DataFrame:
        joined = pd.merge(
            self.user,
            self.doctrine,
            on="item",
            how="outer",
            suffixes=["_user", "_doctrine"],
        ).fillna(value=0)
        joined[["quantity_user", "quantity_doctrine"]] = joined[
            ["quantity_user", "quantity_doctrine"]
        ].astype(int)
        diff = joined.assign(diff=joined["quantity_user"] - joined["quantity_doctrine"])
        return diff.sort_values("item")

    def _create_summary_tables(self) -> dict:
        correct = self.diff.query("diff == 0")[
            ["item", "quantity_doctrine"]
        ].reset_index(drop=True)
        correct = correct.rename(columns={"quantity_doctrine": "quantity"})

        def create_summary(df, operator):
            summary = df.query(f"diff {operator} 0")[["item", "diff"]].reset_index(
                drop=True
            )
            summary = summary.rename(columns={"diff": "quantity"})
            summary["quantity"] = summary["quantity"].abs()
            return summary

        needed = create_summary(self.diff, "<")
        extra = create_summary(self.diff, ">")

        return {"correct": correct, "needed": needed, "extra": extra}


diff = FitDiff(
    user="examples/multibuy.txt",
    doctrine="examples/contents.tsv",
)
print(str(diff))
