import pandas as pd

from data_sources import EXPENDITURES_MOP, MEMBERS_OF_PARLIAMENT_XML

from definitions import Caucus, MOP
from definitions import ContractClaim, Expenditures, HospitalityClaim, TravelClaim, Trip
from definitions import PROPERTIES_LIST, RELATIONS_LIST

from linker import EntityLinker
from multiprocess import BatchProcessor, process_pickle, _categorize_data
from parsers import ExpenseProcessor, MOPProcessor, ClaimsProcessor, TripProcessor
from utils.utils import pickle_load


def get_caucus_data():
    return pd.read_csv("data/csv/caucus.csv", header=0)

def gather_claims():
    type_map= {
            "hospitality": HospitalityClaim,
            "contract": ContractClaim,
            "travel": TravelClaim
        }

    claims_data= pickle_load("data/raw/expense_claims.pkl")
    claims_data= _categorize_data(claims_data)

    data= [
        (n_k, n_v, type_map[t]) 
        for t in claims_data.keys() 
            for n_k,n_v in claims_data[t].items() 
        if n_v is not None]
    
    contract_claims= [_ for _ in data if _[2] == ContractClaim and _[1] is not None]
    hospitality_claims= [_ for _ in data if _[2] == HospitalityClaim and _[1] is not None]
    travel_claims= [_ for _ in data if _[2] == TravelClaim and _[1] is not None]
    trips= [(_[0], _[1], Trip) for _ in data if _[2] == TravelClaim and _[1] is not None]

    return contract_claims, hospitality_claims, travel_claims, trips

def get_claims_data():
    contract_claims, hospitality_claims, travel_claims, trips= gather_claims()
    all_data= [contract_claims, hospitality_claims, travel_claims, trips]
    ret_data= {}

    for _, e_type in zip(all_data, ["ContractClaim", "HospitalityClaim", "TravelClaim", "Trip"]):
        batch_processor = BatchProcessor(_, process_pickle)
        results = batch_processor.process_batch()
        final_df = pd.concat([pd.concat(inner_list) for inner_list in results], ignore_index=True)
        ret_data.update({e_type: final_df})
    
    return ret_data

def get_expense_data():
    df= pd.DataFrame()
    for url in EXPENDITURES_MOP:
        expense= ExpenseProcessor(url, Expenditures)
        expense.run()
        df= pd.concat([df, expense.objs], ignore_index=True)
        
    return df

def get_mops_data():
    mops= MOPProcessor(MEMBERS_OF_PARLIAMENT_XML, MOP)
    mops.run()
    return mops.objs

def properties_table(df):
    features= [col for col in df.columns if col in PROPERTIES_LIST]
    return df[features]

def relations_table(df):
    features= [rel for rel in df.columns if rel in RELATIONS_LIST]
    rel_df= pd.DataFrame()
    for rel in features:
        rels= pd.DataFrame({
            'head': df['id'],
            'relation': rel,
            'tail': df[rel]
        })
        rel_df = pd.concat([rel_df, rels], ignore_index=True)
    return rel_df


class Pipeline:
    def __init__(self):
        self.linker= EntityLinker()

        #   ((one-to-one mapping), (one-to-many))
        self.rel1= ("MemberOfParliament", "memberOf", "Caucus", "hasMember")
        self.rel2= ("Expenditure", "reportedBy", "MemberOfParliament", "reportedExpense")
        self.rel3= ("ContractClaim", "reportedBy", "MemberOfParliament", "reportedClaim")
        self.rel4= ("HospitalityClaim", "reportedBy", "MemberOfParliament", "reportedClaim")
        self.rel5= ("TravelClaim", "reportedBy", "MemberOfParliament", "reportedClaim")
        self.rel6= ("Trip", "reportedBy", "MemberOfParliament", "reportedTrip")
        self.rel7= ("Expenditure", "caucus", "Caucus", "associatedExpense")

        self.rels= [self.rel1, self.rel2, self.rel3, self.rel4, self.rel5, self.rel6, self.rel7]

        self.rel8= ("ContractClaim", "isPartOf", "Expenditure", "hasPart")
        self.rel9= ("HospitalityClaim", "isPartOf", "Expenditure", "hasPart")
        self.rel10= ("TravelClaim", "isPartOf", "Expenditure", "hasPart")
        self.rel11= ("Trip", "isPartOf", "TravelClaim", "subTrip")
        
        self.rels_by_match= [self.rel8, self.rel9, self.rel10]

        self.output_data= {}
    
    def extract(self):
        entities= {
            "Caucus": get_caucus_data(),
            "Expenditure": get_expense_data(),
            "MemberOfParliament": get_mops_data()
        }

        claims= get_claims_data()
        entities.update(claims)

        self.output_data= entities

    def transform(self, *args):

        self.linker.one_to_one_id_mapping(self.output_data[args[0]], args[1])
        self.linker.one_to_many_mapping(
            df=self.output_data[args[2]],
            relation=args[3],
            groupies=self.output_data[args[0]],
            group_by=args[1]
        )
    
    def transform_by_matching_features(self, *args, on):
        self.linker.update_df1(
            self.output_data[args[0]], 
            self.output_data[args[2]], 
            relation= args[1],
            on=on
        )

        self.linker.one_to_many_mapping(
            df=self.output_data[args[2]],
            relation=args[3],
            groupies=self.output_data[args[0]],
            group_by=args[1]
        )
    
    def load(self):
        rels= pd.DataFrame()
        for k, v in self.output_data.items():
            #PROPERTIES= properties_table(v)
            v.to_csv(f'entities/csv/{k}.csv')
            #RELATIONS= relations_table(v)
            #rels= pd.concat([rels, RELATIONS], ignore_index=True)
        #rels.to_csv("entities/csv/relations.csv")
    
    def run(self):
        self.extract()

        for rel in self.rels:
            self.transform(*rel)
        
        for rel in self.rels_by_match:
            self.transform_by_matching_features(*rel, on=["reportedBy", "quarter"])
        
        self.transform_by_matching_features(*self.rel11, on="claimId")

        self.load()

if __name__=="__main__":
    pipe=Pipeline()
    pipe.run()

