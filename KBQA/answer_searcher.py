import collections
import logging
import math
import time
from py2neo import Graph
import question_parser
import heapq
import tiktoken

# from py2neo.packages.httpstream import http
# http.socket_timeout = 9999

import threading
from queue import Queue

class AnswerSearcher:
    def __init__(self, encoding_name: str ='cl100k_base', max_length=2000, timeout=15):
        # self.g = Graph("bolt://192.168.1.35:7687", name="neo4j", password="neo") 
        self.g = Graph("bolt://8.217.152.28:7687", name="neo4j", password="neo4j") 

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
            
        rel_pool = ["包含", "发布", "基本面", "按产品分类", "按地区分类", "按行业分类", "板块", "评价", "按名次划分", "控股"]
        self.rel_triple = collections.defaultdict(list)
        self.rel_triple_hlt = collections.defaultdict(list)
        for rel in rel_pool:
            rel_data = self.g.run(f"MATCH ()-[r:`{rel}`]->() RETURN distinct labels(startNode(r))[0] as h, labels(endNode(r))[0] as t").data()
            for data in rel_data:
                self.rel_triple[rel].append([data['h'], data['t']])
                self.rel_triple_hlt[data['h']].append(data['t'])
                
        # attr_data = self.g.run('CALL db.propertyKeys()').data()
        # attr_pool = [item['propertyKey'] for item in attr_data]

        # import json
        # js = json.dumps(ent_pool, ensure_ascii=False)
        # with open('ent_pool.json', 'w') as file:
        #     file.write(js)
        # js = json.dumps(rel_pool, ensure_ascii=False)
        # with open('rel_pool.json', 'w') as file:
        #     file.write(js)

        self.knowledge = {'个股': stuck_pool, 'code': code_pool, '行业': industry_pool, '研报_发布时间': yanbao_time_pool, 
                     '财务指标_财报': season_pool, '财务指标_发布时间': fin_time_pool, '近一年': latest_time, '实体': ent_pool, 
                     "单节点属性": ent2attr, '关系': rel_pool, '关系三元组': self.rel_triple, '关系三元组辅助': self.rel_triple_hlt, '属性': attr_pool}
        
        self.QP = question_parser.QuestionParser(self.knowledge)
        self.encoding = tiktoken.get_encoding(encoding_name)
        self.max_length = max_length
        self.timeout = timeout # 设置查询超时时间（以秒为单位）
        self.truncation = 5
        self.is_trunc = False
        
    def check_length(self, output):
        return True if len(self.encoding.encode(output)) > self.max_length else False

    def search_main(self, question, ent_dict):
        
        # 识别要不要截断输出 待完善
        # tmp_cnt = 0
        # for i in ent_dict['主体']['股票'] + ent_dict['主体']['行业']:
        #     if i:
        #         tmp_cnt += 1
        if len(ent_dict['主体']['股票']) > 1 or len(ent_dict['发布时间'])>1 or len(ent_dict['intent'])>2:
            self.is_trunc = True
        else:
            self.is_trunc = False
            
        # bleu 和 编辑距离 用于计算相似度
        def bleu(ner, ent):
            """计算抽取实体与现有实体的匹配度 ner候选词,ent查询词"""
            len_pred, len_label = len(ner), len(ent)
            k = min(len_pred, len_label)
            if k == 0:
                return 0
            score = math.exp(min(0, 1 - len_label / len_pred))
            # score = 0
            flag = False
            for n in range(1, k + 1):
                num_matches, label_subs = 0, collections.defaultdict(int)
                for i in range(len_label - n + 1):
                    label_subs[" ".join(ent[i : i + n])] += 1
                for i in range(len_pred - n + 1):
                    if label_subs[" ".join(ner[i : i + n])] > 0:
                        num_matches += 1
                        flag = True
                        label_subs[" ".join(ner[i : i + n])] -= 1  # 不重复
                if not flag and num_matches == 0:  # 一次都没匹配成功
                    score = 0
                    break
                elif num_matches == 0:  # 进行到最大匹配后不再计算
                    break
                score *= math.pow(num_matches / (len_pred - n + 1), math.pow(0.5, n))
            return score if score > 0 else 0

        def editing_distance(word1, word2):
            try:
                m, n = len(word1), len(word2)
            except:
                return float('inf')
            
            if m == 0 or n==0:
                return abs(m-n)
            dp = [[float('inf') for _ in range(n+1)] for _ in range(m+1)]

            for i in range(m):
                dp[i][0] = i

            for i in range(n):
                dp[0][i] = i
            
            for i in range(1, m+1):
                for j in range(1, n+1):

                    if word1[i-1] == word2[j-1]:
                        dp[i][j] = dp[i-1][j-1]
                    else:
                        # 替换
                        dp[i][j] = dp[i-1][j-1] + 1
                        # 删除
                        dp[i][j] = min(dp[i][j], min(dp[i-1][j], dp[i][j-1]) + 1)
            return dp[-1][-1]

        def equal(word1_list: list, word2: str)->bool:
            if word2 in word1_list:
                return True
            return False
        
        def match_helper(match_subject, subject):
            '''更新basic_ent[subject]'''
            scores_best = float('inf') 
            flag = True # 标志是否创建新的一个
            for kg_subject in self.knowledge[subject]:
                if equal(match_subject, kg_subject):
                    if flag:
                        basic_ent[subject].append(kg_subject)
                    else:
                        basic_ent[subject][-1] = kg_subject 
                    break                
                scores_cur = editing_distance(kg_subject, match_subject[0]) if not subject.startswith('财务指标_发布时间') \
                    else -bleu(kg_subject[:len(match_subject[0])], match_subject[0])
                if scores_cur < scores_best:
                    if flag:
                        basic_ent[subject].append(kg_subject)
                        flag = False
                    else:
                        basic_ent[subject][-1] = kg_subject 
                    scores_best = scores_cur

        basic_ent = collections.defaultdict(list)
        
        #　抽取词 ---> KG中的主体（股票， 行业）和发布时间
        extraction_stuck = ent_dict['主体'].get('股票', [])
        extraction_industry = ent_dict['主体'].get('行业', [])
        if extraction_stuck: # 选出最匹配的个股主体
            for e_stuck in extraction_stuck:
                match_helper([e_stuck], '个股')
                if bleu(basic_ent['个股'][-1], e_stuck) == 0:
                    basic_ent['个股'].pop()
                    
        if extraction_industry: # 选出最匹配的行业主体
            for e_industry in extraction_industry:
                match_helper([e_industry], '行业')
                if bleu(basic_ent['行业'][-1], e_industry) == 0:
                    basic_ent['行业'].pop()

        ent_dict['主体']['股票'] = basic_ent['个股']
        ent_dict['主体']['行业'] = basic_ent['行业']
        
        times_fin, times_gudong = {}, {}
        for tp, subjects in ent_dict['主体'].items():
            if tp == '股票': 
                tp = '个股' 
            for subject in subjects:
                if subject:
                    time = self.g.run(f"match (n:`{tp}`)-[r:基本面]-(m:`财务指标`) where n.name='{subject}' return collect(m.发布时间) as time").data()
                    time_gudong = self.g.run(f"match (n:`{tp}`)-[r:基本面]-(m:`十大股东`) where n.name='{subject}' return collect(m.发布时间) as time").data()
                    
                    times_fin[subject] = time[0]['time']
                    times_gudong[subject] = time_gudong[0]['time']
                    
        sql_dict = self.QP.question2sql(question, ent_dict, (times_fin, times_gudong))
            
        # def write_prop(labl: str, prop: dict, no_rep: set):
        #     ret = ''
        #     if labl not in no_rep and prop: # 
        #         ret += f"{labl}包括以下属性：{','.join([key for key in prop.keys()])}\n"
        #         no_rep.add(labl)         
        #     return ret
        
        # def write_prop_desc(labl: str, prop: dict, no_rep_desc):
        #     ret = ''
        #     if labl and prop:
        #         ret_tmp = ';'.join(list(map(str, [val for val in prop.values()])))
        #         if ret_tmp not in no_rep_desc:
        #             no_rep_desc.add(ret_tmp)
        #             ret = f"存在{labl}，属性值为:{ret_tmp}\n"
        #     return ret
        
        def write_prop_tabular(labl: str, prop: dict, no_rep: set, is_trunc: bool=False):
            ret = ''
            if labl and prop:
                truc = self.truncation if is_trunc else len(prop)  
                ret_tmp = f'存在{labl}如下表所示\n|属性|值|\n|--|--|\n' + '\n'.join([f"|{k}|{v}|" for k, v in list(prop.items())[:truc]])+'\n'
                if ret_tmp not in no_rep:
                    # if is_trunc: # 先取全部 后截取 截取后加入集合
                    # no_rep.add(ret_tmp) 
                    ret = ret_tmp
            return ret
        def write_prop_tabular_fin(labl: str, prop: dict, no_rep: set, is_trunc: bool=False):
            ret = ''
            if labl and prop:
                truc = self.truncation if is_trunc else len(prop)  
                ret_tmp = f'存在财务指标-{labl}如下表所示\n|属性|值|\n|--|--|\n' + '\n'.join([f"|{k}|{v}|" for k, v in list(prop.items())[:truc]])+'\n'
                if ret_tmp not in no_rep:
                    # if is_trunc: # 先取全部 后截取 截取后加入集合
                    # no_rep.add(ret_tmp) 
                    ret = ret_tmp
            return ret

        def run_query(query, queue, stop_event):
            result = self.g.run(query)
            try:
                result = self.g.run(query)
            except Exception as e:
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
                print("查询超时")
                stop_event.set() 
                # thread.join()
                res = None
            else:
                res = result_queue.get()
            return res

        def prioritize_key(prop_node: dict, key: list):
            '''优先显示key集合的属性'''
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
        for desc_prefix, sql in sql_dict['node']:
            res = get_res(sql)
            # res = self.g.run(sql).data()
            if res:
                # if desc_prefix not in no_rep:
                #     output += desc_prefix
                #     no_rep.add(desc_prefix)
                output += desc_prefix
                no_rep.add(desc_prefix)
                
                tmp_output = "" 
                tmp_output_trunc = ""
                for r in res:
                    labl, prop = list(r.values())
                    prop = prioritize_key(prop, ['name', '发布时间', '分类类型', '个股'])

                    # node_res.append((labl, prop))
                    # output += write_prop(labl, prop, no_rep)
                    
                    if labl in self.rel_triple_hlt['财务指标']:
                        ret1 = write_prop_tabular_fin(labl, prop, no_rep, True)
                        ret2 = write_prop_tabular_fin(labl, prop, no_rep, self.is_trunc)
                    else:
                        ret1 = write_prop_tabular(labl, prop, no_rep, True)
                        ret2 = write_prop_tabular(labl, prop, no_rep, self.is_trunc)
                    tmp_output_trunc += ret1
                    tmp_output += ret2
                    no_rep.add(ret1)
                    no_rep.add(ret2)

                if len(tmp_output_trunc + tmp_output)==0 or desc_prefix in no_rep:
                    output = output[:-len(desc_prefix)]
                
                if self.check_length(output+tmp_output):
                    output += f"请注意，以下只截取了前{self.truncation}个属性\n"
                    output += tmp_output_trunc if not self.is_trunc else tmp_output
                    return output
                else:
                    output += tmp_output


        # for labl, prop in node_res:
        #     # output += write_prop_tabular(labl, prop, no_rep_desc)
        #     if labl in [i[-1] for i in self.rel_triple['包含']]:
        #         output += write_prop_tabular_fin(labl, prop, no_rep)
        #     else:
        #         output += write_prop_tabular(labl, prop, no_rep)
        # if self.check_length(output+tmp_output+tmp_output_desc) or self.is_trunc:
        #     output += f"请注意，以下只截取了前{self.truncation}个属性\n"
        #     output += tmp_output_trunc            
        #     return output
        # else:
        #     output += tmp_output+tmp_output_desc

        for desc_prefix, sql in sql_dict['path']:
            # print(sql)
            # res = test(sql)
            res = get_res(sql)
            # res = self.g.run(sql).data()
            if res:
                # if desc_prefix not in no_rep:
                #     output += desc_prefix
                #     no_rep.add(desc_prefix)
                output += desc_prefix
                no_rep.add(desc_prefix)
                
                tmp_output_desc = ""  # 等到循环表示的查询统计结束 再考虑是否将属性值放到output中.
                tmp_output = "" 
                tmp_output_trunc = ""
                for r in res:
                    labl_rel, prop_rel, labl_node, prop_node = list(r.values())
                    prop_node = prioritize_key(prop_node, ['name', '发布时间', '分类类型', '个股'])
                    
                    # rel_res.append((labl_rel, prop_rel, labl_node, prop_node))
                    if labl_rel not in no_rep:
                        for h, t in self.rel_triple[labl_rel]:# 打印关系描述
                            output += f"存在{labl_rel}关系：由{h}指向{t}\n"
                            # no_rep.add((h, labl_rel, t))
                            if self.check_length(output):
                                return output
                        no_rep.add(labl_rel)
                    
                    output += write_prop_tabular(labl_rel, prop_rel, no_rep) 

                    if labl_node in ['研报', '财务指标', '主营构成', '十大股东']:
                        continue
                    
                    if labl_node in [i[-1] for i in self.rel_triple['包含']]:
                        ret1 = write_prop_tabular_fin(labl_node, prop_node, no_rep, True)
                        ret2 = write_prop_tabular_fin(labl_node, prop_node, no_rep, self.is_trunc)
                    else:
                        ret1 = write_prop_tabular(labl_node, prop_node, no_rep, True)
                        ret2 = write_prop_tabular(labl_node, prop_node, no_rep, self.is_trunc)
                    tmp_output_trunc += ret1
                    tmp_output += ret2
                    no_rep.add(ret1)
                    no_rep.add(ret2)

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
                    
                if len(tmp_output_trunc + tmp_output)==0 or desc_prefix in no_rep:
                    output = output[:-len(desc_prefix)]

                # cur_length = output+tmp_output_trunc+tmp_output_desc if self.is_trunc else output+tmp_output+tmp_output_desc
                cur_length =  output+tmp_output+tmp_output_desc
                if self.check_length(cur_length): #  or self.is_trunc
                    output += f"请注意，以下只截取了前{self.truncation}个属性\n"
                    
                    output += tmp_output_trunc # if self.is_trunc else tmp_output
                    
                    # for tmp in tmp_output.split('\n')[:-1]:
                    #     output += ';'.join(tmp.split(',')[:self.truncation]) + '\n'
                    # for tmp in tmp_output_desc.split('\n')[:-1]:
                    #     output += ';'.join(tmp.split(';')[:self.truncation]) + '\n'
                    
                    return output
                else:
                    output += tmp_output # tmp_output_trunc if self.is_trunc else tmp_output

        # for labl_rel, prop_rel, labl_node, prop_node in rel_res:
        #     output += write_prop_desc(labl_rel, prop_rel, no_rep_desc) #
        #     output += write_prop_desc(labl_node, prop_node, no_rep_desc)
        #     if self.check_length(output):
        #         return output
        
        return output
    
if __name__ == '__main__':
    AS = AnswerSearcher(max_length=2000, timeout=5) # 在2020年3月31日的

    question = '光洋股份的每股指标和具体的基本每股收益是多少'
    
    logging.basicConfig(filename='answer2.log', level=logging.INFO, format='%(message)s')

    # 1
    ent_dict = {'主体': {'股票': ['广发证券', '光大证券'], '行业': ['']}, '发布时间': ['2022年', '2023年'], 'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(1, end-start, len(answer))
    logging.info(answer)

    # 2
    ent_dict = {'主体': {'股票': ['东方财富'], '行业': ['']}, '发布时间': [''], 'intent': ['实际控制人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(2, end-start, len(answer))
    logging.info(answer)
    # 3
    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, '发布时间': ['2022年'], 'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(3, end-start, len(answer))
    logging.info(answer)
    # 4
    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年', '2021年'], # '2020年3月31日', 
            'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(4, end-start, len(answer))
    logging.info(answer)
    # 5
    ent_dict = {'主体': {'股票': ['东方财富'], '行业': ['']}, 
            "发布时间": ['2022年', '2021年'], # '2020年3月31日', 
            'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(5, end-start, len(answer))
    logging.info(answer)
    # 6
    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2023年3月'], # '2020年3月31日', 
            'intent': ['基本面']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(6, end-start, len(answer))
    logging.info(answer)
    # 7
    ent_dict = {'主体': {'股票': [''], '行业': ['医疗服务']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['成分股']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(7, end-start, len(answer))
    logging.info(answer)
    # 8
    ent_dict = {'主体': {'股票': [''], '行业': ['化学制药']}, 
            "发布时间": ['2022年'], # '2020年3月31日', 
            'intent': ['']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(8, end-start, len(answer))
    logging.info(answer)
    # 9
    ent_dict = {'主体': {'股票': [''], '行业': ['化学制药']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['板块']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(9,end-start, len(answer))
    logging.info(answer)
    # 10
    ent_dict = {'主体': {'股票': [''], '行业': ['化学制药']}, 
            "发布时间": ['2022年'], # '2020年3月31日', 
            'intent': ['评价']}  # 失败
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(10, end-start, len(answer))
    logging.info(answer)
    # 11
    ent_dict = {'主体': {'股票': ['中国中免'], '行业': ['']}, 
            "发布时间": ['2022年'], # '2020年3月31日', 
            'intent': ['基本面']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(11, end-start, len(answer))
    logging.info(answer)
    # 12
    ent_dict = {'主体': {'股票': ['泸州老窖', '洋河股份'], '行业': ['']}, 
            "发布时间": ['2022年', '2021年'], # '2020年3月31日', 
            'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(12, end-start, len(answer))
    logging.info(answer)
    # 13
    ent_dict = {'主体': {'股票': ['分众传媒'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日',   fail
            'intent': ['实控人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(13, end-start, len(answer))
    logging.info(answer)
    # 14
    ent_dict = {'主体': {'股票': ['贵州茅台'], '行业': ['']}, 
        "发布时间": ['2022年'], # '2020年3月31日', 
        'intent': ['净利润']} # '财务指标', 
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(14, end-start, len(answer))
    logging.info(answer)
    # 15
    ent_dict = {'主体': {'股票': ['协和电子'], '行业': ['']}, 
        "发布时间": ['2020年3月31日'], # '2020年3月31日', 
        'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(15, end-start, len(answer))
    logging.info(answer)
    # 16
    ent_dict = {'主体': {'股票': ['协和电子'], '行业': ['']}, 
        "发布时间": ['2020年3月31日'], # '2020年3月31日', 
        'intent': ['基本每股收益']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(16, end-start, len(answer))
    logging.info(answer)
    # 17
    ent_dict = {'主体': {'股票': ['光洋股份'], '行业': ['航空航天', '航空机场', '包装材料']}, 
                "发布时间": [''], # '2020年3月31日', 
                'intent': ['基本每股收益', '财务指标', '每股指标', '板块']}
    answer = AS.search_main(question, ent_dict)
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(17, end-start, len(answer))
    logging.info(answer)

    # 18
    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['基本面']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(18, end-start, len(answer))
    logging.info(answer)
    # 19
    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['板块']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(19, end-start, len(answer))
    logging.info(answer)
    # 20
    ent_dict = {'主体': {'股票': ['长电科技'], '行业': ['']}, 
        "发布时间": [''], # '2020年3月31日', 
        'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(20, end-start, len(answer))
    logging.info(answer)
    # 21
    ent_dict = {'主体': {'股票': ['长电科技'], '行业': ['']}, 
        "发布时间": [''], # '2020年3月31日', 
        'intent': ['主营信息']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(21, end-start, len(answer))
    logging.info(answer)
    # 22
    ent_dict = {'主体': {'股票': ['长电科技'], '行业': ['']}, 
        "发布时间": [''], # '2020年3月31日', 
        'intent': ['行业']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(22, end-start, len(answer))
    logging.info(answer)
    # 23
    ent_dict = {'主体': {'股票': ['协和电子'], '行业': ['']}, 
            "发布时间": ['2020年3月31日'], # '2020年3月31日', 
            'intent': ['营业总收入']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(23, end-start, len(answer))
    logging.info(answer)
    # 24
    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日',   失败 时间没对上
            'intent': ['股东信息']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(24, end-start, len(answer))
    logging.info(answer)
    # 25
    ent_dict = {'主体': {'股票': ['N赛维'], '行业': ['']}, 
            "发布时间": [''], # '2020年3月31日', 
            'intent': ['股东信息', '情况', '可以', '控股人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(25, end-start, len(answer))
    logging.info(answer)
    # 26
    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年', '2023'], # '2020年3月31日', 
            'intent': ['财务指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(26, end-start, len(answer))
    logging.info(answer)
    # 27
    ent_dict = {'主体': {'股票': ['浙江仙通'], '行业': ['']}, 
            "发布时间": ['2022年', '2023'], # '2020年3月31日', 
            'intent': ['实控人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(27, end-start, len(answer))
    logging.info(answer)
    # 28
    ent_dict = {'主体': {'股票': ['浙江仙通'], '行业': ['']}, 
            "发布时间": ['2022年', '2023'], # '2020年3月31日', 
            'intent': ['控股人']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(28, end-start, len(answer))
    logging.info(answer)
    # 29
    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年9月'], # '2022年', '2023', 
            'intent': ['评级']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(29, end-start, len(answer))
    logging.info(answer)
    # 30
    ent_dict = {'主体': {'股票': ['广发证券'], '行业': ['']}, 
            "发布时间": ['2022年9月'], # '2022年', '2023', 
            'intent': ['基本面']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(30, end-start, len(answer))
    logging.info(answer)

    # 31
    ent_dict = {'主体': {'股票': ['光洋股份'], '行业': ['航空航天', '航空机场', '包装材料']}, 
                "发布时间": ['2023'], # '2020年3月31日', 
                'intent': ['基本每股收益',]}
    answer = AS.search_main(question, ent_dict)
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(31, end-start, len(answer))
    logging.info(answer)
    
    # 32
    ent_dict = {'主体': {'股票': ['广发证券', '光大证券'], '行业': ['']}, '发布时间': ['2022年', '2023年'], 'intent': ['常用指标']}
    start = time.time()
    answer = AS.search_main(question, ent_dict)
    end = time.time()
    print(32, end-start, len(answer))
    logging.info(answer)

