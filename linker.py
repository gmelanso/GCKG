
import pandas as pd


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
