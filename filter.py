import pandas as pd

def filter_property_table(df, objs):
    return df[df['id'].isin(set(objs['head']).union(objs['tail']))]


objs= pd.read_csv("entities/csv/relations.csv", header=0)

claim_to_mop= objs.query("relation == 'reportedBy' and head.str.contains('ContractClaim')")
mop_to_caucus= objs.query("relation == 'memberOf'")

relations= pd.concat([claim_to_mop, mop_to_caucus], ignore_index=True)

caucus, claims, mops= pd.read_csv("entities/csv/Caucus.csv"), \
            pd.read_csv("entities/csv/ContractClaim.csv"), \
            pd.read_csv("entities/csv/MemberOfParliament.csv")

caucus, claims, mops= [filter_property_table(table, relations) for table in [caucus, claims, mops]]

caucus.to_csv("entities/csv/caucus_props.csv")
claims.to_csv("entities/csv/contract_props.csv")
mops.to_csv("entities/csv/mop_props.csv")
relations.to_csv("entities/csv/rels.csv")
