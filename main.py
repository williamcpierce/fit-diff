import pandas as pd


def parse_contents(filepath: str) -> pd.DataFrame():
    contents = pd.read_table(
        filepath,
        header=None,
        names=["item", "type", "location", "quantity"],
        usecols=["item", "quantity"],
    )
    contents["item"] = contents["item"].apply(lambda x: x.rstrip())
    contents["quantity"] = pd.to_numeric(contents["quantity"], downcast="integer")
    contents = contents.groupby("item")["quantity"].sum().reset_index(name="quantity")
    return contents


def parse_multibuy(filepath: str) -> pd.DataFrame():
    return (
        pd.read_table("examples/multibuy.txt", header=None)[0]
        .str.split(r"([\s])[x](?=\d)", expand=True)
        .drop(1, axis="columns")
        .rename(columns={0: "item", 2: "quantity"})
        .replace([None], value=0)
        .groupby("item")["quantity"]
        .sum()
        .reset_index(name="quantity")
    )


class FitDiff:
    def __init__(self, user: pd.DataFrame(), doctrine: pd.DataFrame()):
        self.user = user
        self.doctrine = doctrine
        self.joined = self.diff_quantity()
        self.correct, self.needed, self.extra = self.summary_tables()

    def __str__(self) -> str:
        return (
            f"{'='*60}\nCorrect:\n"
            f"{self.correct.to_string()}\n\n{'='*60}\n"
            "Needed:\n"
            f"{self.needed.to_string()}\n\n{'='*60}\n"
            "Extra:\n"
            f"{self.extra.to_string()}"
        )

    def diff_quantity(self) -> pd.DataFrame():
        joined = (
            pd.merge(
                self.user,
                self.doctrine,
                how="outer",
                on="item",
            )
            .fillna(value=0)
            .rename(columns={"quantity_x": "user_qty", "quantity_y": "doctrine_qty"})
        )
        joined[["user_qty", "doctrine_qty"]] = joined[
            ["user_qty", "doctrine_qty"]
        ].apply(lambda x: pd.to_numeric(x, downcast="integer"))
        joined["diff"] = joined["user_qty"] - joined["doctrine_qty"]
        return joined

    def summary_tables(self) -> list[pd.DataFrame()]:
        correct = (
            self.joined[(self.joined["diff"] == 0)]
            .drop(["user_qty", "diff"], axis="columns")
            .rename(columns={"doctrine_qty": "quantity"})
            .reset_index(drop=True)
        )

        needed = (
            self.joined[(self.joined["diff"] < 0)]
            .drop(["user_qty", "doctrine_qty"], axis="columns")
            .rename(columns={"diff": "quantity"})
            .reset_index(drop=True)
        )
        needed[["quantity"]] = needed[["quantity"]].apply(abs)

        extra = (
            self.joined[(self.joined["diff"] > 0)]
            .drop(["user_qty", "doctrine_qty"], axis="columns")
            .rename(columns={"diff": "quantity"})
            .reset_index(drop=True)
        )
        return (correct, needed, extra)


diff = FitDiff(
    user=parse_multibuy("examples/multibuy.txt"),
    doctrine=parse_contents("examples/contents.tsv"),
)
print(str(diff))
