
import pandas as pd
from dataclasses import dataclass

from definitions import MOP, Expenditures
from parsers import MOPProcessor, ExpenseProcessor
from data_sources import MEMBERS_OF_PARLIAMENT_XML, EXPENDITURES_MOP


class EntityLinker:
    def __init__(self):

        self.aliases_to_id= pd.read_csv("data/csv/aliases.csv", header=0).set_index('name')['id'].to_dict()

    def one_to_one_id_mapping(self, df, relation):
        df[relation]= df[relation].map(self.aliases_to_id)
    
    def one_to_many_mapping(self, df, relation, groupies, group_by):

        neighbourhood= {
            group: group_df["id"].tolist() for group, group_df in groupies.groupby(group_by)
            }
        
        if relation not in df.columns:
            df[relation]= df["id"].map(neighbourhood)
        else:
            df[relation]= df[relation] + df["id"].map(neighbourhood)    

    def update_df1(self, df1, df2, relation, on):
        # Merge dataframes on specified columns
        merged = pd.merge(df1, df2, on=on, how='left', suffixes=('_df1', '_df2'))
        df1[relation] = merged['id_df2']


class MOPLinker(EntityLinker):
    def __init__(self):
        super().__init__()
    
    def _to_caucus(self, mops, caucus):
        self.rels_by_alias(mops, "memberOf")
        self.rels_by_group_map(caucus, "hasMember", mops, "memberOf")


class ExpenseLinker(EntityLinker):
    def __init__(self):
        super().__init__()

    def _to_mops(self, expenses, mops):
        self.rels_by_alias(expenses, "reportedBy")
        self.rels_by_group_map(mops, "reportedExpense", expenses, "reportedBy")

    def _to_caucus(self, expenses, caucus):
        self.rels_by_alias(expenses, "caucus")
        self.rels_by_group_map(caucus, "associatedMOPExpense", expenses, "caucus")
        

"""caucus= pd.read_csv("data/raw/caucus.csv", header=0)
MOPs= MOPProcessor(MEMBERS_OF_PARLIAMENT_XML, MOP)
MOPs.run()

expenses = pd.concat([ExpenseProcessor(url, Expenditures).run().objs for url in EXPENDITURES_MOP], ignore_index=True)

linkerm= MOPLinker()
linker= ExpenseLinker()
"""
"""linkerm._to_caucus(MOPs.objs, caucus)
print(MOPs.objs["memberOf"])
print(caucus["hasMember"])"""


    
