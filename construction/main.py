import collections
from py2neo import Graph, Node
from pymysql.converters import escape_string
import json
import sys 
import os
os.chdir(sys.path[0])
from tqdm import tqdm



def main(seedKG_path='../FR2KG/seedKG', make_new=True):

    '''构建基础图谱'''

    
    g = Graph('bolt://192.168.1.35:7687', name = 'neo4j', password='neo')

    # if make_new:
    #     # 先删除原有图谱
    #     g.run(f"match ()-[r]-() delete r")
    #     g.run(f"match (n) delete n")

    with open(os.path.join('../FR2KG/schema.json'), 'r') as f:
        schema = json.load(f)
    with open(os.path.join(seedKG_path, 'entities.json'), 'r') as f:
        ents = json.load(f)
    with open(os.path.join(seedKG_path, 'relationships.json'), 'r') as f:
        rels = json.load(f)
    with open(os.path.join(seedKG_path, 'attributes.json'), 'r') as f:
        ents_attr = json.load(f)

    for label, names in tqdm(ents.items()):
        for name in names:
            name = escape_string(name)
            g.run(f"create (n: `{label}`) set n.name='{name}'")

    for lst in tqdm(list(ents_attr.values())[0]):
        ent, attr, desc = list(map(lambda x:escape_string(x), lst))
        # 查询该schema中的有该属性的实体类型
        for ent_label, attrs  in schema['attrs'].items():
            if attr in attrs.keys():
                g.run(f"match (n:`{ent_label}`) where n.name='{ent}' set n.{attr}='{desc}'")
                break

    for lst in tqdm(list(rels.values())[0]):
        ent_head, cur_rel, ent_tail = list(map(lambda x:escape_string(x), lst))
        # 查询该schema中有该关系的实体类型
        for label_head, rel, label_tail in schema['relationships']:
            if rel==cur_rel:
                g.run(f"match (n:`{label_head}`), (m:`{label_tail}`) where n.name='{ent_head}' and m.name='{ent_tail}' \
                    create (n)-[r:`{rel}`]->(m)")

    
if __name__ == '__main__':
    seedKG_path = '../FR2KG/evaluationKG'
    main(seedKG_path)
