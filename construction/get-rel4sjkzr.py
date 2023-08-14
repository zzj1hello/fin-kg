
import sys 
import os
import pandas as pd

os.chdir(sys.path[0])
import multiprocessing

import csv
from py2neo import Graph, Node

def main(data):
    g = Graph('bolt://192.168.1.35:7687', name = 'neo4j', password='neo')
    # split_data = data.split(',')
    # split_data = [item.strip('"') for item in split_data]
    # stuck, code = split_data[:2]
    # holders = split_data[2:-1]
    # holder_ratio = split_data[-1]

    g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `实际控制人`) REQUIRE (n.name, n.个股) IS Unique")

    for stuck, code, holders, holder_ratio in data:
    
        if holders:
            node_properties = {'name':holders, '个股': stuck, '代码':code, '实际控制人':holders, '持股比例': holder_ratio if holder_ratio else '无'}
            try:
                g.create(Node("实际控制人",**node_properties))
            except:
                pass
            g.run(f"match (n:`个股`), (p:`实际控制人`) where n.name='{stuck}' and p.个股='{stuck}' and p.实际控制人='{holders}' \
                        merge (p)-[r:`控股`]->(n)")
            
if __name__ == '__main__':

    with open('../crawl/数据/实际控制人.csv', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        # 跳过表头行
        next(reader)
        # 将每一行数据转换为列表
        data = [row for row in reader]
    main(data)