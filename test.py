from openai import OpenAI
client = OpenAI()

response = client.chat.completions.create(
  model="gpt-4-0125-preview",
  messages=[{
      "role":"user",
      "content":"""
    You are an expert using Neo4j V5.13.0. 
    Write a cypher query to load a CSV of relations to a database where the nodes have been loaded.
    The CSV has 3 columns: head and tail are strings that are == to node ids, and relation is a string predicate
    to be used as display labels for the edge. The file is very large and therefore the cypher query
    will need to be computationally efficient."""}]
)

print(response)