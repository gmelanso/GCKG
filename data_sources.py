
MEMBERS_OF_PARLIAMENT_XML= 'https://www.ourcommons.ca/Members/en/search/XML'

EXPENDITURES_MOP= [
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2024/2/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2024/1/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2023/4/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2023/3/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2023/2/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2023/1/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2022/4/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2022/3/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2022/2/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2022/1/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2021/4/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2021/3/csv',
    'https://www.ourcommons.ca/ProactiveDisclosure/en/members/2021/2/csv'
]

"""
transformer= NGSIv2Transformer(definition=MOP)
MOPs.objs["json"]= MOPs.objs.apply(lambda row: transformer.tf_pandas(row), axis=1)
print(MOPs.objs)

json_data = json.dumps(MOPs.objs['json'].tolist(), indent=2)

# Write the JSON string to a file
with open('output.json', 'w') as json_file:
    json_file.write(json_data)"""




