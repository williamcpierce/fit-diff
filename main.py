import pandas as pd

contents = pd.read_table(
    "examples/contents.tsv",
    header=None,
    names=["item", "type", "location", "quantity"],
    usecols=["item", "quantity"],
)
contents["item"] = contents["item"].apply(lambda x: x.rstrip())
# print(f"contents:\n{contents}")

multibuy = pd.read_table("examples/multibuy.txt", header=None, names=["raw"])
multibuy["parsed"] = multibuy["raw"].str.split(r"([\s])[x](?=\d)")
multibuy["item"] = multibuy["parsed"].apply(lambda x: x[0])
multibuy["quantity"] = pd.to_numeric(
    multibuy["parsed"].apply(lambda x: x[2] if len(x) == 3 else 1)
)
multibuy.drop(columns=["raw", "parsed"], inplace=True)
# print(f"multibuy:\n{multibuy}")


joined = pd.merge(
    contents.groupby("item")["quantity"].sum(),
    multibuy.groupby("item")["quantity"].sum(),
    how="outer",
    on=["item"],
)

print(joined)
