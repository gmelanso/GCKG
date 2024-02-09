import re
import requests
import numpy as np
import pandas as pd
import uuid

from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO

from utils.utils import is_valid_url, get_rows_between_indices


class PreProcessor:
    def __init__(self, url, definition):
        self.url= url
        self.definition= definition
        self.type= definition.TYPE
        self.relations= definition.RELS
        self.properties= definition.PROPS
    
    def _rename_features(self, df=None):
        if hasattr(self.definition, 'ATTR_MAP'):
            if df is None:
                self.objs.rename(columns= self.definition.ATTR_MAP, inplace=True)
            else:
                df.rename(columns= self.definition.ATTR_MAP, inplace=True)
    
    def drop_extra_features(self):
        cols= set(self.objs.columns).intersection(self.relations + self.properties)
        self.objs= self.objs[list(cols)]

    def get_urn(self, entity_type):
        return f'urn:gckg:{entity_type}:{str(uuid.uuid4())}'
    
    def get_date_created(self):
        return datetime.now().isoformat()
    
    def get_archive(self):
        return self.url
    
    def add_additional_features(self, df=None, **kwargs):
        if df is None:
            self.objs= self.objs.assign(
                archivedAt= self.get_archive(),
                dateCreated= self.get_date_created(),
                type= self.type,
                **kwargs)
        else:
            df= df.assign(
                archivedAt= self.get_archive(),
                dateCreated= self.get_date_created(),
                type= self.type,
                **kwargs)


class XMLParser(PreProcessor):
    def __init__(self, url, definition):
        super().__init__(url, definition)
        self.type= self.definition.TYPE

    def _read_xml(self):
        self.objs= pd.read_xml(self.url)
        self.objs = self.objs.applymap(lambda x: [] if pd.isna(x) else x)


class CSVProcessor(PreProcessor):
    def __init__(self, url, definition):
        super().__init__(url, definition)
        self.type= self.definition.TYPE
    
    def _read_csv(self):
        if self.url is not None:
            if is_valid_url(self.url):
                self.objs= pd.read_csv(self.url)
            else:
                temp = pd.read_csv(StringIO(self.csv.decode('utf-8'))).replace(np.nan, None)
                temp.reset_index(inplace=True)
                self.extract_metadata(temp.iloc[0].to_string())
                col_names, data = temp.iloc[0], temp.iloc[1:]

                self.objs = pd.DataFrame(data.values, columns=col_names)
        else:
            return


class ClaimsProcessor(PreProcessor):

    def __init__(self, url, definition, csv):
        super().__init__(url, definition)
        self.type= self.definition.TYPE
        self.csv= csv
    
    def _read_claims_csv(self):
        temp = pd.read_csv(StringIO(self.csv.decode('utf-8'))).replace(np.nan, None)
        temp.reset_index(inplace=True)
        self.extract_metadata(temp.iloc[0].to_string())
        col_names, data = temp.iloc[0], temp.iloc[1:]

        self.objs = pd.DataFrame(data.values, columns=col_names)

    def extract_metadata(self, metadata):
        if metadata is None:
            print("Error: Metadata is None.")
            return None, None

        pattern = re.compile(r'– (?P<name>[^–]+) – (?P<quarter>[A-Za-z0-9]+) (?P<year>\d{4})')
        match = pattern.search(metadata)

        if match:
            name, quarter, year = match.group('name'), match.group('quarter'), int(match.group('year'))
            # Subtract 1 from the year
            year -= 1
            self.reportedBy= name
            self.quarter= f"{year}{quarter}"

        return
    
    def run(self):
        self._read_claims_csv()
        self._rename_features()
        self.drop_extra_features()
        self.add_additional_features(
            id= lambda x: x['type'].apply(lambda et: self.get_urn(et)),
            reportedBy= self.reportedBy,
            quarter= self.quarter
        )


class ExpenseProcessor(CSVProcessor):
    def __init__(self, url, definition):
        super().__init__(url, definition)
        self.type= self.definition.TYPE

    def _get_expense_date(self):
        match = re.search(r'/members/(\d{4})/(\d+)/', self.url)
            
        if match:
            year = int(match.group(1)) - 1
            quarter = match.group(2)
            return f"{year}Q{quarter}"
        else:
            return 

    def run(self):
        self._read_csv()
        self._rename_features()
        self.drop_extra_features()
        self.add_additional_features(
            id= lambda x: x['type'].apply(lambda et: self.get_urn(et)),
            quarter= self._get_expense_date()
        )
    

class MOPProcessor(XMLParser):
    def __init__(self, url, definition):
        super().__init__(url, definition)
        self.type= self.definition.TYPE

    def _get_mp_hrefs(self, namespace='https://www.ourcommons.ca', html_cl='ce-mip-mp-tile'):
        """Generates URLs to MPs profiles."""
        url= "https://www.ourcommons.ca/Members/en/search"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        self.hrefs = [namespace + element['href'] for element in soup.find_all(class_=html_cl)]

        return 
        
    def _match_names_to_urls(self, row, hrefs):
        def remove_accents(string):
            accents = {'à': 'a', 'â': 'a', 'ä': 'a', 'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
                       'î': 'i', 'ï': 'i', 'ô': 'o', 'ö': 'o', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ç': 'c'}
            return ''.join(accents.get(char, char) for char in string)

        def split_and_clean_names(names):
            return [remove_accents(part) for name in names if isinstance(name, str)
                    for part in re.split(r'[- ]', name) if part]
        
        row_names = split_and_clean_names([row['givenName'].lower().replace("'", ""), row['familyName'].lower().replace("'", "")])
        valid_hrefs = [url for url in hrefs if url is not None]

        matching_urls = [url for url in valid_hrefs if all(name in url.lower() for name in row_names)]

        return matching_urls[0] if matching_urls else None

    def run(self):
        self._read_xml()
        self._rename_features()
        self.drop_extra_features()
        self._get_mp_hrefs()
        self.add_additional_features(
            id=self.objs.apply(lambda row: self._match_names_to_urls(row, self.hrefs), axis=1)
        )


class TravelProcessor(ClaimsProcessor):

    def __init__(self, url, definition, csv):
        super().__init__(url, definition, csv)
        self.type= self.definition.TYPE
    
    def run(self):
        self._read_claims_csv()
        self.objs= self.objs[self.objs["Traveller Type"].isnull()]
        self._rename_features()
        self.drop_extra_features()
        self.add_additional_features(
            id= lambda x: x['type'].apply(lambda et: self.get_urn(et)),
            reportedBy= self.reportedBy,
            quarter= self.quarter
        )
    
class TripProcessor(ClaimsProcessor):

    def __init__(self, url, definition, csv):
        super().__init__(url, definition, csv)
        self.type= self.definition.TYPE
        self.relations= self.definition.RELS
        self.properties= self.definition.PROPS
    
    def run(self):
        self._read_claims_csv()
        self.objs= self.objs[self.objs["Traveller Type"].notnull()]
        self._rename_features()
        self.drop_extra_features()
        self.add_additional_features(
            id= lambda x: x['type'].apply(lambda et: self.get_urn(et)),
            reportedBy= self.reportedBy,
            quarter= self.quarter
        )
