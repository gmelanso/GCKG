"""Class instances for all gckg entities."""


class Entity:
    """Instance of a singleton defined through inheritance."""

    def __init__(self, data):
        self.data= data


    def _build_entity_attrs(self):
        # Create dictionaries for properties and relationships
        prop_dict = {prop: self._new_property(self.data.get(prop, None)) for prop in self.PROPS}
        rel_dict = {rel: self._new_relation(self.data.get(rel, None)) for rel in self.RELS}

        # Add missing properties and relationships with empty values
        prop_dict.update({prop: self._new_property(None) for prop in set(self.PROPS) - prop_dict.keys()})
        rel_dict.update({rel: self._new_relation(None) for rel in set(self.RELS) - rel_dict.keys()})

        entity_attrs = {**prop_dict, **rel_dict, "type": self.type}
        
        return entity_attrs


    def _new_property(self, val):
        return {
            "type": "Property",
            "value": val if val else []
        }
    

    def _new_relation(self, val):
        if isinstance(val, list):
            return {
                "type": "Relationship",
                "object": val
            }
        else:
            return {
                "type": "Relationship",
                "object": [val] if val else []
            }


class MOP(Entity):

    def __init__(self, data):
        super().__init__(data)
    
    TYPE= "MemberOfParliament"

    PROPS= [
        "id",
        "type",
        "alias",
        "addressRegion",
        "archivedAt",
        "dateCreated",
        "familyName",
        "givenName",
        "honorificPrefix",
        "startDate",
        "endDate"
    ]

    RELS= [
        "memberOf", # Government of Canada caucus
        "reportedClaim",
        "reportedExpense",
        "reportedTrip"
    ]

    ATTR_MAP= {
        'PersonShortHonorific': "honorificPrefix",
        'PersonOfficialFirstName': "givenName", 
        'PersonOfficialLastName': "familyName", 
        'ConstituencyName': "hasConstituency", 
        'ConstituencyProvinceTerritoryName': "addressRegion", 
        'CaucusShortName': "memberOf", 
        'FromDateTime': "startDate", 
        'ToDateTime': "endDate"
    }


class Caucus(Entity):
    def __init__(self, data):
        super().__init__(data)
        
    TYPE= "Caucus"

    PROPS= [
        "id",
        "type",
        "alias",
        "name"
    ]

    RELS= [
        "hasMember",
        "associatedExpense"
    ]


class ContractClaim(Entity):
    def __init__(self, data):
        super().__init__(data)

    TYPE= "ContractClaim"
    
    PROPS= [
        "id",
        "type",
        "archivedAt",
        "dateCreated",
        "description",
        "expenseDate",
        "expenseTotal",
        "quarter",
        "vendor"
    ]

    RELS= [
        "isPartOf",
        "reportedBy"
    ]

    ATTR_MAP= {
        "Supplier": "vendor",
        "Date": "expenseDate",
        "Total": "expenseTotal",
        "Description": "description"
    }


class HospitalityClaim(Entity):
    def __init__(self, data):
        super().__init__(data)
    
    TYPE= "HospitalityClaim"
    
    PROPS= [
        "id",
        "type",
        "typeOfEvent",
        "archivedAt",
        "claimId",
        "dateCreated",
        "municipality",
        "numberOfAttendees",
        "expenseDate",
        "purpose",
        "quarter",
        "expenseTotal",
        "vendor"
    ]

    RELS= [
        "partOfExpense",
        "reportedBy"
    ]

    ATTR_MAP= {
        "Date": "expenseDate",
        "Total of Attendees": "numberOfAttendees",
        "Purpose of Hospitality": "purpose",
        "Type of Event": "typeOfEvent",
        "Claim": "claimId",
        "Total": "totalExpense",
        "Supplier": "vendor",
        "Location": "municipality"
    }


class TravelClaim(Entity):
    def __init__(self, data):
        super().__init__(data)
        
    TYPE= "TravelClaim"
    
    PROPS= [
        "id",
        "type",
        "accommodations",
        "archivedAt",  
        "claimId",      
        "dateCreated",
        "endDate",
        "incidentals",
        "regularPoints",
        "purpose",
        "quarter",
        "specialPoints",
        "startDate",
        "expenseTotal",
        "transportation",
        "usaPoints"
    ]

    RELS= [
        "partOfExpense",
        "subTrip",
        "reportedBy"
    ]

    ATTR_MAP= {
        "Claim": "claimId",
        "Travel start date": "startDate",
        "Travel end date": "endDate",
        "Accommodations": "accomodations",
        "Transportation": "transportation",
        "Meals and Incidentals": "incidentals",
        "Total": "totalExpense",
        "Points Reg.": "regularPoints",
        "Points Spec.": "specialPoints",
        "Points U.S.A.": "usaPoints",
        "Purpose of Travel": "purpose"
    }


class Trip(Entity):
    def __init__(self, data):
        super().__init__(data)
    
    TYPE= "Trip"
    
    PROPS= [
        "id",
        "type",
        "archivedAt",
        "claimId",
        "dateCreated",
        "departureDate",
        "purpose",
        "reportDate",
        "travellerType",
        "tripDestination",
        "tripOrigin",
        "travelledBy"
    ]

    RELS= [
        "isPartOf",
        "reportedBy"
    ]

    ATTR_MAP= {
        "Claim": "claimId",
        "Traveller Type": "travellerType",	
        "Purpose of Travel": "purpose",	
        "Dates": "departureDate",	
        "Departure": "tripOrigin",	
        "Destination": "tripDestination"
    }


class Expenditures(Entity):
    def __init__(self, data):
        super().__init__(data)
    
    TYPE= "Expenditure"

    PROPS= [
        "id",
        "type",
        "alias",
        "archivedAt"
        "contractExpense",
        "dateCreated",
        "expenseDate",
        "hospitalityExpense",
        "salaryExpense",
        "travelExpense",
        "caucus"
    ]

    RELS= [
        "reportedBy",
        "hasPart"
    ]

    ATTR_MAP= {
        "Name": "reportedBy",
        "Salaries": "salaryExpense",
        "Travel": "travelExpense",
        "Hospitality": "hospitalityExpense",
        "Contracts": "contractExpense",
        "Caucus": "caucus",
        "Constituency": "constituency"
    }

PROPERTIES_LIST= Caucus.PROPS + MOP.PROPS + Expenditures.PROPS + ContractClaim.PROPS    \
                + HospitalityClaim.PROPS + TravelClaim.PROPS + Trip.PROPS

RELATIONS_LIST= Caucus.RELS + MOP.RELS + Expenditures.RELS + ContractClaim.RELS    \
                + HospitalityClaim.RELS + TravelClaim.RELS + Trip.RELS

MOP_VOTES= 'https://www.ourcommons.ca/Members/en/votes/xml'


