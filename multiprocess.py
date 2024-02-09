import json
from io import StringIO
import numpy as np
import pandas as pd
from definitions import HospitalityClaim, ContractClaim, TravelClaim, Trip
from parsers import ClaimsProcessor, TravelProcessor, TripProcessor
from utils.utils import pickle_load
import multiprocessing

class BatchProcessor:
    def __init__(self, data, process_func, num_processes=6, chunk_size=None):
        self.data = data
        self.process_func = process_func
        self.num_processes = num_processes
        self.chunk_size = chunk_size or len(data) // self.num_processes

    def _process_chunk(self, chunk):
        results = []
        for item in chunk:
            result = self.process_func(item[0], item[1], item[2])
            results.append(result)
        return results

    def process_batch(self):
        with multiprocessing.Pool(processes=self.num_processes) as pool:
            results = pool.map(self._process_chunk, self._split_data_into_chunks())
        return results

    def _split_data_into_chunks(self):
        chunks = []
        for i in range(0, len(self.data), self.chunk_size):
            chunks.append(self.data[i:i+self.chunk_size])
        return chunks

def process_pickle(url, csv, claim_func):

    if csv is not None:
        if claim_func == Trip:
            claims= TripProcessor(url=url, definition=claim_func, csv=csv)
            claims.run()
            return claims.objs
        if claim_func == TravelClaim:
            claims= TravelProcessor(url=url, definition=claim_func, csv=csv)
            claims.run()
            return claims.objs
        else:
            claims= ClaimsProcessor(url=url, definition=claim_func, csv=csv)
            claims.run()
            return claims.objs

def _categorize_data(urls_dict):
    return {
        category: 
            {
                url: value for url, value in urls_dict.items() if category in url and value is not None
            } 

            for category in ["travel", "contract", "hospitality"]
        }



if __name__ == "__main__":

    type_map= {
        "hospitality": HospitalityClaim,
        "contract": ContractClaim,
        "travel": TravelClaim,

    }

    claims_data= pickle_load("data/expense_claims.pkl")
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
    
    all_data= [contract_claims, hospitality_claims, travel_claims, trips]
    #pickle_data["contract"]= {k:v for k,v in pickle_data["contract"].items() if v is not None}
    for _ in all_data:
        batch_processor = BatchProcessor(_, process_pickle)
        results = batch_processor.process_batch()
        final_df = pd.concat([pd.concat(inner_list) for inner_list in results], ignore_index=True)
        print(final_df.columns)
