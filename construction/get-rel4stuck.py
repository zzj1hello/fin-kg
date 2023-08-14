import re
from datetime import datetime
import collections
from bs4 import BeautifulSoup
from py2neo import Graph, Node
from pymysql.converters import escape_string
import json
import sys 
import os
import pandas as pd
import requests
os.chdir(sys.path[0])
from tqdm import tqdm
import efinance as ef
import csv
import akshare as ak
import logging
import multiprocessing

# 配置日志记录器
logging.basicConfig(filename='api_error.log', level=logging.WARNING) # 出错时记录日志

def main(data, make_new=True):

    '''构建 (个股)-[板块]->(行业) (个股)-[板块]->(地域) (个股)-[板块]->(概念)'''
    g = Graph('bolt://192.168.1.35:7687', name = 'neo4j', password='neo')
    
    # if make_new:
    #     # 先删除原有图谱
    #     g.run(f"match ()-[r]-() delete r")
    #     g.run(f"match (n) delete n")

        
    # g.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:`个股`) REQUIRE n.name IS UNIQUE")
    # g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `行业`) REQUIRE n.name IS UNIQUE")
    # g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `地域`) REQUIRE n.name IS UNIQUE")
    # g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `概念`) REQUIRE n.name IS UNIQUE")

    for stuck, code in tqdm(data): 
        # try:
        #     df = ef.stock.get_belong_board(code)
        #     bankuai_name = df['板块名称'].values
        #     bankuai_code = df['板块代码'].values
        #     stuck_code = df['股票代码'].values[0]

        # except:
        #     continue
        # industry, region, gainians = bankuai_name[0], bankuai_name[1], bankuai_name[2:]
        # industry_code, region_code, gainian_codes = bankuai_code[0], bankuai_code[1], bankuai_code[2:]
        
        ''' 后补实体属性
        g.run(f"match (n) where n.name='{stuck}' set n.code='{stuck_code}'")
        g.run(f"match (n) where n.name='{industry}' set n.code='{industry_code}'")
        g.run(f"match (n) where n.name='{region}' set n.code='{region_code}'")
        for gainian, gainian_code in zip(gainians, gainian_codes):
            g.run(f"match (n) where n.name='{gainian}' set n.code='{gainian_code}'")
        '''
        
        # 添加实体
        # g.run("WITH $stuck as stuck, $stuck_code as stuck_code MERGE (n:`个股` {name: stuck, code: stuck_code})", 
        #       stuck=stuck, stuck_code=stuck_code)
        # g.run("WITH $industry as industry, $industry_code as industry_code MERGE (n:`行业` {name: industry, code: industry_code})", 
        #       industry=industry, industry_code=industry_code)
        # g.run("WITH $region as region $region_code as region_code MERGE (n:`地域` {name: region, code: region_code})", 
        #       region=region, regon_code=region_code)
        # for gainian, gainian_code in zip(gainians, gainian_codes):
        #     g.run("WITH $gainian as gainian, $gainian_code as gainian_code MERGE (n:`概念` {name: gainian, code:gainian_code})", 
        #           gainian=gainian, gainian_code=gainian_code)
            
        # # 构建关系  
        # g.run(f"match (n:`个股`), (m:`行业`) where n.name='{stuck}' and m.name='{industry}'  merge (n)-[r:`板块`]->(m)" )
        # g.run(f"match (n:`个股`), (m:`地域`) where n.name='{stuck}' and m.name='{region}' merge (n)-[r:`板块`]->(m)" )
        # for gainian in gainians:
        #     g.run(f"match (n:`个股`), (m:`概念`) where n.name='{stuck}' and m.name='{gainian}' merge (n)-[r:`板块`]->(m)" )


        # # 构建基本面-财务指标 个股--基本面->财务指标（年报、季报）--包含-> 常用指标、营运能力 、...
        # g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `财务指标`) REQUIRE (n.name, n.个股) IS Unique")
        # try:
        #     basic_finance = ak.stock_financial_abstract(symbol=code) 
        # except Exception as e:
        #     logging.error(f'财务指标 api获取异常：{code}，错误信息：{e}')
        #     continue

        # name_map = {3: '一季报', 6: '二季报', 9: '三季报', 12: '年报'}
        # for report_time in basic_finance.columns[2:]:
        #     fmt = datetime.strptime(report_time, "%Y%m%d")
            
        #     report_type = '年报' if report_time[4:6] == '12' else '季报'
        #     report_name = report_time[:4] + name_map[fmt.month]
        #     report_date = fmt.strftime("%Y-%m-%d")
        #     node_properties = {"name":report_name, "个股": stuck, "发布时间": report_date, "报告类型": report_type}
        #     # g.run(f"match (n) where n.name='{report_date+report_type}' delete n")
        #     try:
        #         g.create(Node("财务指标",**node_properties))
        #     except:
        #         pass
        #     g.run(f"match (n:`个股`), (m:`财务指标`) where n.name='{stuck}' and m.个股='{stuck}' and \
        #             m.name='{report_name}' merge (n)-[r:`基本面`]->(m)")

        #     for choice in set(basic_finance['选项'].values):
        #         data_tmp = basic_finance[['选项', '指标', report_time]][basic_finance['选项']==choice].iloc[:, 1:].values
        #         node_properties = {i: j if str(j) != "nan" else '无' for i, j in data_tmp}
        #         node_properties.update({"name": stuck, "发布时间": report_date})
        #         # g.run(f"match (n) where n.name='{stuck}' and n.发布时间='{report_date}'  delete n")

        #         g.run(f"create CONSTRAINT IF NOT EXISTS FOR (n: `{choice}`) REQUIRE (n.name, n.发布时间) IS Unique")
        #         try: # 防止重复创建
        #             g.create(Node(choice, **node_properties))
        #         except:
        #             pass 
        #         g.run(f"match (m:`财务指标`), (p:`{choice}`) where m.name='{report_name}' and m.个股='{stuck}'\
        #                 and p.name='{stuck}' and p.发布时间='{report_date}' merge (m)-[r:`包含`]->(p)")

            
        # 构建基本面-主营构成  个股--基本面->主营构成（年度、中期）--按行业划分、按产品划分、按地区划分、...->主营指标      
        # g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `主营构成`) REQUIRE (n.name, n.发布时间) IS Unique")
        # try:
        #     basic_business = ak.stock_zygc_em(symbol='sz'+code if code[:2] == '00' or code[:2] == '30' else 'sh'+ code)
        # except Exception as e:
        #     logging.error(f'主营构成 api获取异常：{code}，错误信息：{e}')
        #     continue
        # basic_business['分类类型'] = basic_business['分类类型'].fillna('按行业分类')
        # basic_business['报告日期'] = basic_business['报告日期'].astype(str)

        # for report_period in set(basic_business['报告日期']):
        #     node_properties = {"name": stuck, "发布时间": report_period}
        #     try:
        #         g.create(Node("主营构成",**node_properties))
        #     except:
        #         pass
        #     g.run(f"match (n:`个股`), (m:`主营构成`) where n.name='{stuck}' and m.name='{stuck}' and \
        #             m.发布时间='{report_period}' merge (n)-[r:`基本面`]->(m)")
        #     df_data = basic_business[basic_business['报告日期']==report_period]
        #     attr_names = [i for i in basic_business.columns[4:]]
        #     for rel_name in set(df_data['分类类型']):
        #         data_val = df_data[df_data['分类类型']==rel_name].iloc[:, 3:].values

        #         for d in data_val:
        #             name, vals = escape_string(d[0]), d[1:]
        #             node_properties = dict(zip(attr_names, vals))

        #             # 特别划分 "其中:" 的关系
        #             child_name = re.findall(r'其中:(.*)', name)
        #             if len(child_name)>0:
        #                 name = child_name[0]
                        
        #             # 创建节点
        #             node_properties.update({'name': name, '个股': stuck, '发布时间': report_period, '分类类型': rel_name})
        #             g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `主营指标`) REQUIRE (n.name, n.个股, n.发布时间, n.分类类型) IS Unique")
        #             try:
        #                 g.create(Node("主营指标",**node_properties))
        #             except:
        #                 pass
                    
        #             g.run(f"match (m:`主营构成`), (p:`主营指标`) where m.name='{stuck}' and m.发布时间='{report_period}'\
        #                     and p.name='{name}' and p.个股='{stuck}' and p.发布时间='{report_period}' and \
        #                         p.分类类型 = '{rel_name}' merge (m)-[r:`{rel_name}`]->(p)")
    

        # 个股-[基本面]-十大股东
        g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `十大股东`) REQUIRE (n.name, n.发布时间) IS Unique")
        g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `股东明细`) REQUIRE (n.name, n.发布时间, n.股东名称) IS Unique")
        for data in g.run("match (n:`财务指标`) where n.发布时间 IS NOT NULL return distinct n.发布时间").data():
            date = data['n.发布时间']
            try:
                basic_gudong = ak.stock_gdfx_top_10_em(symbol='sz'+code if code[:2] == '00' or code[:2] == '30' else 'sh'+ code, \
                    date=date.replace("-", ""))
                basic_gudong = basic_gudong.fillna('无')
            except Exception as e:
                logging.error(f'十大股东 api获取异常：{code} {date}，错误信息：{e}')
                continue
            
            node_properties = {'name': stuck, '发布时间': date}
            try:
                g.create(Node("十大股东",**node_properties))
            except:
                pass
            g.run(f"match (m:`个股`), (p:`十大股东`) where m.name='{stuck}' \
                    and p.name='{stuck}' and p.发布时间='{date}'  merge (m)-[r:`基本面`]->(p)")

            for data in basic_gudong.values:
                node_properties = dict(zip(basic_gudong.columns, data))                    
                # 创建节点
                node_properties.update({'name': stuck, '发布时间': date})
                try:
                    g.create(Node("股东明细",**node_properties))
                except:
                    pass
            g.run(f"match (p:`十大股东`), (q:`股东明细`) where p.name='{stuck}' and p.发布时间='{date}' \
                        and q.name='{stuck}' and q.发布时间='{date}'  merge (p)-[r:`按名次划分`]->(q)")
    
        

        # # 循环所有标签并为其添加索引
        # labels = self.g.run("CALL db.labels()").data()

        # for label in labels:
        #     query = f"CREATE INDEX IF NOT EXISTS FOR (n:`{label['label']}`) ON (n.name)"
        #     self.g.run(query)
        #     query = f"CREATE INDEX IF NOT EXISTS FOR (n:`{label['label']}`) ON (n.发布时间)"
        #     self.g.run(query)

if __name__ == '__main__':
    
    
    with open('../crawl/数据/沪深京A股数据.csv', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        # 跳过表头行
        next(reader)
        # 将每一行数据转换为列表
        data = [row for row in reader]
    # main(data)

    num_processes = 10
    chunk_size = len(data) // num_processes
    processes = []
    for i in range(num_processes):
        start = i * chunk_size
        end = start + chunk_size
        if i == num_processes - 1:
            end = len(data)
        process_data = data[start:end]
        p = multiprocessing.Process(target=main, args=(process_data,))
        processes.append(p)
        p.start()
        
    # 等待所有进程执行完毕
    for p in processes:
        p.join()
        
        
    # pool = multiprocessing.Pool(6)
    # pool.map(main, data) 
    # pool.close()

