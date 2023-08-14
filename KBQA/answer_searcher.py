import collections
import logging
import time
from py2neo import Graph
import question_parser
import heapq
import tiktoken

# from py2neo.packages.httpstream import http
# http.socket_timeout = 9999

import threading
from queue import Queue
import timeout_decorator

# import openai
# import os
# os.environ["OPENAI_API_KEY"] = "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"
# os.environ["http_proxy"] = "127.0.0.1:11080"
# os.environ["https_proxy"] = "127.0.0.1:11080"
# openai.api_key = os.getenv('OPENAI_API_KEY')

class AnswerSearcher:
    def __init__(self, encoding_name: str ='cl100k_base', max_length=2000, timeout=15):
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
        
        # ent_pool = ["研报", "行业", "机构", "个股", "十大股东", "股东明细", "实际控制人", "地域", "概念", "财务指标", "每股指标", "盈利能力", 
        #                  "财务风险", "常用指标", "营运能力", "成长能力", "收益质量"]
        ent_data = self.g.run("CALL db.labels() YIELD label").data()
        ent_pool = [item['label'] for item in ent_data]
        
        ent2attr = {}
        attr_pool = set()        # 返回节点属性名
        for ent in ent_pool:
            tmp = set(self.g.run(f"match (n:`{ent}`) return properties(n) limit 1").data()[0]['properties(n)'])
            attr_pool |= tmp
            ent2attr[ent] = tmp
            
        rel_pool = ["包含", "发布", "基本面", "按产品分类", "按地区分类", "按行业分类", "板块", "评价", "按股东名次划分", "控股"]
        self.rel_triple = collections.defaultdict(list)
        for rel in rel_pool:
            rel_data = self.g.run(f"MATCH ()-[r:`{rel}`]->() RETURN distinct labels(startNode(r))[0] as h, labels(endNode(r))[0] as t").data()
            for data in rel_data:
                self.rel_triple[rel].append([data['h'], data['t']])
            
        attr_data = self.g.run('CALL db.propertyKeys()').data()
        # attr_pool = [item['propertyKey'] for item in attr_data]
        
        knowledge = {'个股': stuck_pool, 'code': code_pool, '行业': industry_pool, '研报_发布时间': yanbao_time_pool, 
                     '财务指标_财报': season_pool, '财务指标_发布时间': fin_time_pool, '近一年': latest_time, '实体': ent_pool, 
                     "单节点属性": ent2attr, '关系': rel_pool, '关系三元组': self.rel_triple, '属性': attr_pool}
        
        self.QP = question_parser.QuestionParser(knowledge)
        self.encoding = tiktoken.get_encoding(encoding_name)
        self.max_length = max_length
        self.timeout = timeout # 设置查询超时时间（以秒为单位）
        self.truncation = 5
        
    def check_length(self, output):
        return True if len(self.encoding.encode(output)) > self.max_length else False

    def search_main(self, question, ent_dict):
        sql_dict = self.QP.question2sql(question, ent_dict)
        
        def write_prop(labl: str, prop: dict, no_rep: set):
            ret = ''
            if labl not in no_rep and prop: # 
                ret += f"{labl}包括以下属性：{','.join([key for key in prop.keys()])}\n"
                no_rep.add(labl)         
            return ret
        
        def write_prop_desc(labl: str, prop: dict, no_rep_desc):
            ret = ''
            if labl and prop:
                ret_tmp = ';'.join(list(map(str, [val for val in prop.values()])))
                if ret_tmp not in no_rep_desc:
                    no_rep_desc.add(ret_tmp)
                    ret = f"存在{labl}，属性值为:{ret_tmp}\n"
            return ret
        
        def write_prop_tabular(labl: str, prop: dict, no_rep: set, is_trunc: bool=False):
            ret = ''
            if labl and prop:
                truc = self.truncation if is_trunc else len(prop)  
                ret_tmp = f'存在{labl}如下表所示\n|属性|值|\n|--|--|\n' + '\n'.join([f"|{k}|{v}|" for k, v in list(prop.items())[:truc]])+'\n'
                if ret_tmp not in no_rep:
                    no_rep.add(ret_tmp)
                    ret = ret_tmp
            return ret
        def write_prop_tabular_fin(labl: str, prop: dict, no_rep: set, is_trunc: bool=False):
            ret = ''
            if labl and prop:
                truc = self.truncation if is_trunc else len(prop)  
                ret_tmp = f'存在财务指标-{labl}如下表所示\n|属性|值|\n|--|--|\n' + '\n'.join([f"|{k}|{v}|" for k, v in list(prop.items())[:truc]])+'\n'
                if ret_tmp not in no_rep:
                    no_rep.add(ret_tmp)
                    ret = ret_tmp
            return ret

        def run_query(query, queue, stop_event):
            result = self.g.run(query)
            try:
                result = self.g.run(query)
            except Exception as e:
                print("查询超时:", e)
                stop_event.set()  # 设置停止事件
            finally:
                pass

            queue.put(result)

        def get_res(sql):
            result_queue = Queue()
            # 创建停止事件对象
            stop_event = threading.Event()
            thread = threading.Thread(target=run_query, args=(sql, result_queue, stop_event))
            thread.setDaemon(True) # 将线程设置为守护线程，当主线程终止时，守护线程也会被强制终止。
            
            thread.start() # 启动线程
            thread.join(self.timeout) # 等待线程执行完毕或超时
            if thread.is_alive(): # 判断线程是否仍在运行
                # print("查询超时")
                stop_event.set() 
                # thread.join()
                res = None
            else:
                res = result_queue.get()
            return res

        def prioritize_key(prop_node: dict, key: list):
            new_dic = {}
            for k in key:
                if k in prop_node:
                    v = prop_node.pop(k)
                    new_dic[k] = v
            new_dic.update(**prop_node)
            return new_dic
                
        output = ''
        no_rep = set() # 相同的节点类型提前声明有哪些属性prop 且不重复
        no_rep_desc = set() # 相同的prop描述 不重复
        node_res = [] # 将查询结果缓存
        rel_res = []
        for sql in sql_dict['node']:
            # res = test(sql)
            res = get_res(sql)
            # res = self.g.run(sql).data()
            if res:
                for r in res:
                    labl, prop = list(r.values())
                    prop = prioritize_key(prop, ['name', '发布时间', '分类类型'])

                    node_res.append((labl, prop))
                    output += write_prop(labl, prop, no_rep)
        for labl, prop in node_res:
            output += write_prop_desc(labl, prop, no_rep_desc)
            if labl in [i[-1] for i in self.rel_triple['包含']]:
                output += write_prop_tabular_fin(labl, prop, no_rep)
            else:
                output += write_prop_tabular(labl, prop, no_rep)

        for sql in sql_dict['path']:
            # print(sql)
            # res = test(sql)
            res = get_res(sql)
            # res = self.g.run(sql).data()
            if res:
                tmp_output_desc = ""  # 等到循环表示的查询统计结束 再考虑是否将属性值放到output中.
                tmp_output = ""
                tmp_output_trunc = ""
                for r in res:
                    labl_rel, prop_rel, labl_node, prop_node = list(r.values())
                    prop_node = prioritize_key(prop_node, ['name', '发布时间', '分类类型'])
                    
                    # rel_res.append((labl_rel, prop_rel, labl_node, prop_node))
                    if labl_rel not in no_rep:
                        for h, t in self.rel_triple[labl_rel]:# 打印关系描述
                            output += f"存在{labl_rel}关系：由{h}指向{t}\n"
                            # no_rep.add((h, labl_rel, t))
                            if self.check_length(output):
                                return output
                        no_rep.add(labl_rel)
                    
                    output += write_prop_tabular(labl_rel, prop_rel, no_rep)
                    if labl_node in [i[-1] for i in self.rel_triple['包含']]:
                        tmp_output += write_prop_tabular_fin(labl_node, prop_node, no_rep)
                        tmp_output_trunc += write_prop_tabular_fin(labl_node, prop_node, no_rep, True)
                    else:
                        tmp_output += write_prop_tabular(labl_node, prop_node, no_rep)
                        tmp_output_trunc += write_prop_tabular(labl_node, prop_node, no_rep, True)
                        
                    # if labl_rel not in no_rep: # 打印关系属性
                    #     # output += write_prop(labl_rel, prop_rel, no_rep)
                    #     tmp_output += write_prop(labl_rel, prop_rel, no_rep)
                    #     no_rep.add(labl_rel)
                    # if labl_node not in no_rep: # 打印节点属性
                    #     # output += write_prop(labl_node, prop_node, no_rep)
                    #     tmp_output += write_prop(labl_node, prop_node, no_rep)
                    #     no_rep.add(labl_node)
                    # tmp_output_desc += write_prop_desc(labl_rel, prop_rel, no_rep_desc)
                    # tmp_output_desc += write_prop_desc(labl_node, prop_node, no_rep_desc)
                    
                if self.check_length(output+tmp_output+tmp_output_desc):
                    output += f"请注意，以下只截取了前{self.truncation}个属性\n"
                    
                    output += tmp_output_trunc
                    
                    # for tmp in tmp_output.split('\n')[:-1]:
                    #     output += ';'.join(tmp.split(',')[:self.truncation]) + '\n'
                    # for tmp in tmp_output_desc.split('\n')[:-1]:
                    #     output += ';'.join(tmp.split(';')[:self.truncation]) + '\n'
                    
                    return output
                else:
                    output += tmp_output+tmp_output_desc

        # for labl_rel, prop_rel, labl_node, prop_node in rel_res:
        #     output += write_prop_desc(labl_rel, prop_rel, no_rep_desc) #
        #     output += write_prop_desc(labl_node, prop_node, no_rep_desc)
        #     if self.check_length(output):
        #         return output
        
        return output
    
if __name__ == '__main__':
    AS = AnswerSearcher(max_length=2000, timeout=5) # 在2020年3月31日的
    logging.basicConfig(filename='answer.log', level=logging.INFO, format='%(message)s')

    question = '光洋股份的每股指标和具体的基本每股收益是多少'
    
    ent_dict = {'主体': {'股票': [''], '行业': ['化学制药']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['板块']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': [''], '行业': ['化学制药']}, 
            "发布时间": ['2022年'], # '2020年3月31日', 
            'intent': ['评价']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['中国中免'], '行业': ['']}, 
            "发布时间": ['2022年'], # '2020年3月31日', 
            'intent': ['基本面']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['泸州老窖', '洋河股份'], '行业': ['']}, 
            "发布时间": ['2022年', '2021年'], # '2020年3月31日', 
            'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['分众传媒'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['实控人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['贵州茅台'], '行业': ['']}, 
        "发布时间": ['2022年'], # '2020年3月31日', 
        'intent': ['净利润']} # '财务指标', 
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    # ent_dict = {'主体': {'股票': ['协和电子'], '行业': ['']}, 
    #     "发布时间": ['2020年3月31日'], # '2020年3月31日', 
    #     'intent': ['财务指标']}
    # start = time.time()
    # answer = AS.search_main(question, ent_dict)
    # end = time.time()
    # print(end-start, len(answer))
    # logging.info(answer)

    ent_dict = {'主体': {'股票': ['协和电子'], '行业': ['']}, 
        "发布时间": ['2020年3月31日'], # '2020年3月31日', 
        'intent': ['基本每股收益']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    # ent_dict = {'主体': {'股票': ['光洋股份'], '行业': ['航空航天', '航空机场', '包装材料']}, 
    #             "发布时间": [''], # '2020年3月31日', 
    #             'intent': ['基本每股收益', '财务指标', '每股指标', '板块']}
    # answer = AS.search_main(question, ent_dict)
    # start = time.time()
    # answer = AS.search_main(question, ent_dict)
    # end = time.time()
    # print(end-start, len(answer))
    # logging.info(answer)


    # ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
    #         "发布时间": [''], # '2020年3月31日', 
    #         'intent': ['基本面']}
    # start = time.time()
    # answer = AS.search_main(question, ent_dict)
    # end = time.time()
    # print(end-start, len(answer))
    # logging.info(answer)

    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['板块']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['长电科技'], '行业': ['']}, 
        "发布时间": [''], # '2020年3月31日', 
        'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['长电科技'], '行业': ['']}, 
        "发布时间": [''], # '2020年3月31日', 
        'intent': ['主营信息']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['长电科技'], '行业': ['']}, 
        "发布时间": [''], # '2020年3月31日', 
        'intent': ['行业']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['协和电子'], '行业': ['']}, 
            "发布时间": ['2020年3月31日'], # '2020年3月31日', 
            'intent': ['营业总收入']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['股东信息']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['股东信息', '情况', '可以', '控股人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年', '2023'], # '2020年3月31日', 
            'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    
    ent_dict = {'主体': {'股票': ['浙江仙通'], '行业': ['']}, 
            "发布时间": ['2022年', '2023'], # '2020年3月31日', 
            'intent': ['控股人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年9月'], # '2022年', '2023', 
            'intent': ['评级']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年9月'], # '2022年', '2023', 
            'intent': ['基本面']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(end-start, len(answer))
    logging.info(answer)

