from langchain.chat_models import ChatOpenAI
from langchain.chains import GraphCypherQAChain
from langchain.graphs import Neo4jGraph

graph = Neo4jGraph(
    url="bolt://192.168.1.35:7688", username="neo4j", password="neo4jneo4j"
)


print(graph.get_schema)


chain = GraphCypherQAChain.from_llm(
    ChatOpenAI(temperature=0), graph=graph, verbose=True
)

chain.run("2023-06-26这个时间的研报对电子行业的评级怎么样?")

