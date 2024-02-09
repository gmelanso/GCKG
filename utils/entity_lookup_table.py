import csv
import json
import os

import pandas as pd

from data_sources import MEMBERS_OF_PARLIAMENT_XML, EXPENDITURES_MOP
from definitions import MOP, Expenditures
from parsers import MOPProcessor, ExpenseProcessor


def get_caucus_aliases():
    with open("./entities/json/Caucus.json") as file:
        caucus= json.load(file)

    return pd.DataFrame([{"name": name, "id": c["id"]} for c in caucus for name in c["alias"]["value"]])



def get_agent_aliases():
    MOPs = MOPProcessor(MEMBERS_OF_PARLIAMENT_XML, MOP)
    MOPs.run()
    MOPs = MOPs.objs[["givenName", "familyName", "id"]]

    df1 = pd.DataFrame({
        "name": MOPs["familyName"] + ", " + MOPs["givenName"],
        "id": MOPs["id"]
    })

    df2 = pd.DataFrame({
        "name": MOPs["familyName"] + ", Hon. " + MOPs["givenName"],
        "id": MOPs["id"]
    })

    df3 = pd.DataFrame({
        "name": MOPs["givenName"] + " " + MOPs["familyName"],
        "id": MOPs["id"]
    })

    df4 = pd.DataFrame({
        "name": MOPs["id"],
        "id": MOPs["id"]
    })

    return pd.concat([df1, df2, df3, df4], ignore_index=True)

def get_expense_aliases():
    expense_df= pd.DataFrame()
    for url in EXPENDITURES_MOP:
        expenses= ExpenseProcessor(url, Expenditures)
        expenses.run()
        new_df= pd.DataFrame({
            "name": expenses.objs["id"],
            "id": expenses.objs["id"]
        })
        expense_df= pd.concat([expense_df, new_df], ignore_index=True)
    return expense_df

aliases= pd.concat([get_caucus_aliases(), get_agent_aliases(), get_expense_aliases()], ignore_index=True)
aliases.to_csv("./data/csv/aliases.csv", index=False)


        


 


