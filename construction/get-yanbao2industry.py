import collections
from py2neo import Graph, Node
from pymysql.converters import escape_string
import json
import sys 
import os
import pandas as pd
os.chdir(sys.path[0])
from tqdm import tqdm
import csv



def main(make_new=True):

    '''构建 (机构)-[发布]->(研报)-[评级]->(行业)'''

    
    g = Graph('bolt://192.168.1.35:7688', name = 'neo4j', password='neo4jneo4j')

    if make_new:
        # 先删除原有图谱
        g.run(f"match ()-[r]-() delete r")
        g.run(f"match (n) delete n")

    schema = { "entity_type": [
                            "个股",
                            "行业",
                            '地域',
                            '概念',
                            "研报",
                            '证券公司',

                            "机构",
                            "风险",
                            "文章",
                            "指标",
                            "品牌",
                            "业务",
                            "产品",
                        ],
                "attrs": {
                    "研报": {
                        "发布时间": "date",
                    },
                },
                "relationships": [
                        [
                            "机构",
                            "发布",
                            "研报"
                        ],
                        [
                            "研报",
                            "评级",
                            "行业"
                        ],
                        [
                            "研报",
                            "评级",
                            "个股"
                        ],

                        [
                            "个股",
                            "板块",
                            "行业"
                        ],
                        [
                            "个股",
                            "板块",
                            "地域"
                        ],
                        [
                            "个股",
                            "板块",
                            "概念"
                        ],

                ]
            }
    # 打开csv文件
    with open('../data/行业数据/研报/行业研报基本信息.csv', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        # 跳过表头行
        next(reader)
        # 将每一行数据转换为列表
        data = [row for row in reader]

    g.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:`机构`) REQUIRE n.name IS UNIQUE")
    g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `研报`) REQUIRE (n.name, n.发布时间) IS UNIQUE")
    g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `行业`) REQUIRE n.name IS UNIQUE")

    for line in tqdm(data):
        date, industry, title, ratefrom, rate_last, rate_cur = [escape_string(line[i]) for i in [0, 1, 3, 4, 6, 7]]
        g.run("WITH $ratefrom as ratefrom MERGE (n:`机构` {name: ratefrom})", ratefrom=ratefrom)
        g.run("WITH $title as title, $date as date MERGE (m:`研报` {name: title, 发布时间: date})", title=title, date=date)
        g.run("WITH $industry as industry MERGE (p:`行业` {name: industry})", industry=industry)

        g.run(f"match (n:`机构`), (m:`研报`) where n.name='{ratefrom}' and m.name='{title}' \
            merge (n)-[rf:`发布`]->(m)" )
        g.run(f"match (m:`研报`), (p: `行业`) where m.name='{title}' and p.name='{industry}' \
            merge (m)-[rp:`评级`]->(p) set rp.上次评级='{rate_last if rate_last else '无'}', rp.评级='{rate_cur if rate_cur else '无'}' " )
        
        # g.run(f"match (n:`机构`), (m:`研报`), (p: `行业`), where n.name='{ratefrom}' and m.name='{title}' and p.name='{industry}' \
        #     merge (n)-[rf:`发布`]->(m)-[rp:`评级`]->(p) set rp.上次评级='{rate_last if rate_last else '无'}', rp.评级='{rate_cur if rate_cur else '无'}' " )

if __name__ == '__main__':
    main()
