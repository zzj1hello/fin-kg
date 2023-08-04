import collections
from py2neo import Graph
import question_parser
import heapq

class AnswerSearcher:
    def __init__(self):
        self.g = Graph("bolt://192.168.1.35:7687", name="neo4j", password="neo")
        stuck_data = self.g.run(f"match (n:`个股`) return n.name, n.code").data()
        stuck_pool, code_pool = list(zip(*[(item['n.name'], item['n.code']) for item in stuck_data]))
        industry_data = self.g.run(f"match (n:`行业`) return n.name").data()
        industry_pool = [item['n.name'] for item in industry_data]
        
        yanbao_time_data = self.g.run(f"match (n:`研报`) return n.发布时间").data()
        yanbao_time_pool = set([item['n.发布时间'] for item in yanbao_time_data])
        
        fin_time_data = self.g.run(f"match (n:`财务指标`) where n.发布时间 IS NOT NULL return n.发布时间, n.name").data()
        season_pool, fin_time_pool = list(zip(*[(item['n.name'], item['n.发布时间']) for item in fin_time_data]))
        latest_time = heapq.nlargest(3, set(fin_time_pool))
        
        ent_pool = ["研报", "行业", "机构", "个股", "地域", "概念", "每股指标" ,
                         "财务风险", "常用指标", "营运能力", "成长能力", "收益质量"]
        ent4ret = {"财务指标": {"每股指标", "盈利能力", "财务风险", "常用指标", "营运能力", "成长能力", "收益质量"},
                   "主营构成": {"主营指标"}}
        ent2attr = {}
        for ent in ent_pool:
            ent2attr[ent] = set(self.g.run(f"match (n:`{ent}`) return properties(n) limit 1").data()[0]['properties(n)'])
            
        rel_pool = ["发布", "基本面", "按产品分类", "按地区分类", "按行业分类", "板块", "评价"]
        rel_triple = collections.default(list)
        for rel in rel_pool:
            rel_data = self.g.run(f"MATCH ()-[r:`{rel}`]->() RETURN labels(startNode(r))[0] as h, labels(endNode(r))[0] as t limit 1").data()[0]
            rel_triple[rel].append([rel_data['h'], rel_data['t']])
            
        attr_data = self.g.run('CALL db.propertyKeys()').data()
        attr_pool = [item['propertyKey'] for item in attr_data]
        
        knowledge = {'个股': stuck_pool, 'code': code_pool, '行业': industry_pool, '研报_发布时间': yanbao_time_pool, 
                     '财务指标_财报': season_pool, '财务指标_发布时间': fin_time_pool, '近一年': latest_time, '实体': ent_pool, 
                     "单节点属性": ent2attr, '关系': rel_pool, '关系三元组': rel_triple, '属性': attr_pool}
        
        self.QP = question_parser.QuestionParser(knowledge)
    
    
    def search_main(self, question, ent_dict):
        sqls_list = self.QP.question2sql(question, ent_dict)
        
        output = ''
        for sql in sqls_list:
            res = self.g.run(sql).data()
            if res:
                for r in res:
                    labl, prop = r['labels'], r['properties']
                    output += f"{labl[0]}的信息："
                    for key, value in prop.items():
                        output += f"{key}为{value};"
                    output += '\n'
        return output
                
        # output = ''
        # for sql, desc in sqls_list.items():
        #     res = self.g.run(sql).data()
        #     check_rep = set()
        #     if res:
        #         a, b, c = desc.split()
        #         if a.startswith('properties'):
        #             output += f'{c}\n'
        #             for obj in res:
        #                 for key, val in obj[a].items():
        #                     if val is None or val in check_rep:
        #                         continue
        #                     check_rep.add(val)

        #                     if not key in b.split(','):
        #                         output += f'{key}: {val};'
        #             output += '\n'
        #         elif a.startswith('n.'):
        #             output += f'{c}:'
        #             for obj in res:
        #                 for key, val in obj.items():
        #                     if val is None or val in check_rep:
        #                         continue
        #                     check_rep.add(val)
                            
        #                     if not key in b.split(','):
        #                         output += f'{val};'
        #             output += '\n'
        # return output
    
if __name__ == '__main__':
    AS = AnswerSearcher()
    question = '光洋股份在2020年3月31日的每股指标和具体的基本每股收益是多少'
    ent_dict = {'主体': {'股票': ['光洋股份'], '行业': ['航空航天', '航空机场', '包装材料']}, 
                "发布时间": [''], # '2020年3月31日', 
                'intent': ['基本每股收益', '财务指标', '每股指标']}
    answer = AS.search_main(question, ent_dict)