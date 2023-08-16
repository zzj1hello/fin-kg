import collections
from copy import deepcopy
from datetime import datetime
import math
from itertools import combinations
import heapq

# import json

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


class QuestionParser:
    def __init__(self, knowledge):
        self.most_k_similar = 3
        self.knowledge = knowledge
        self.max_return = 15 # 对路径查询限制路径条数 
        self.max_return_2 = 5
        
        # stuck_data = json.dumps(self.knowledge['个股'], ensure_ascii=False)
        # with open('stuck_data.json', 'w') as file:
        #     file.write(stuck_data)
            
        # indu_data = json.dumps(self.knowledge['行业'])
        # with open('indu_data.json', 'w') as file:
        #     file.write(indu_data)

    def question2sql(self, question, ent_dict, times_all):
        
        sql_dict = collections.defaultdict(list) # {返回类型: 查询语句}

        basic_ent = collections.defaultdict(list) # 要查询的主体、发布时间、意图信息

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
                scores_cur = editing_distance(kg_subject, match_subject[0]) if not '发布时间' in subject \
                    else -bleu(kg_subject[:len(match_subject[0])], match_subject[0])
                if scores_cur < scores_best:
                    if flag:
                        basic_ent[subject].append(kg_subject)
                        flag = False
                    else:
                        basic_ent[subject][-1] = kg_subject 
                    scores_best = scores_cur


        # #　抽取词 ---> KG中的主体（股票， 行业）和发布时间
        # extraction_stuck = ent_dict['主体'].get('股票', [])
        # extraction_industry = ent_dict['主体'].get('行业', [])
        # if extraction_stuck: # 选出最匹配的个股主体
        #     for e_stuck in extraction_stuck:
        #         match_helper([e_stuck], '个股')
        #         if bleu(basic_ent['个股'][-1], e_stuck) == 0:
        #             basic_ent['个股'].pop()
                    
        # if extraction_industry: # 选出最匹配的行业主体
        #     for e_industry in extraction_industry:
        #         match_helper([e_industry], '行业')
        #         if bleu(basic_ent['行业'][-1], e_industry) == 0:
        #             basic_ent['行业'].pop()

        basic_ent['个股'] = ent_dict['主体'].get('股票', [])
        basic_ent['行业'] = ent_dict['主体'].get('行业', [])


        # 选出最匹配的时间 为空则使用近一年的
        extraction_time = ent_dict.get('发布时间')
        times_fin, times_gudong = times_all       
        
        def find_time(index_type, time_pool):
            if len(extraction_time) == 0 or extraction_time[0] == '':
                for subject, sj_time in time_pool.items():
                    if sj_time:
                        basic_ent[f'{index_type}_{subject}'] = heapq.nlargest(3, sj_time)
                    else:
                        basic_ent[f'{index_type}_{subject}'] = ['2022', '2023', ''] # heapq.nlargest(3, sj_time)
            else:
                for subject, sj_time in time_pool.items():
                    self.knowledge[f'{index_type}_{subject}'] = sj_time
                    for e_time in extraction_time: # 保证用户想要的时间是 图谱中有该主体最接近的时间
                        if e_time:
                            e_time_trans = [] # 精确查询
                            try: # 用户给到某年某月某日  精确
                                ymd = datetime.strptime(e_time, "%Y年%m月%d日").strftime("%Y-%m-%d")
                                e_time_trans.append(ymd)
                                if ymd.endswith('12-31') and ymd in sj_time:
                                    basic_ent[f'{index_type}_{subject}'].append(ymd) # 精确
                                else:
                                    e_time_trans.append(ymd) 

                            except:
                                try: # 用户给到某年某月 模糊
                                    ym = datetime.strptime(e_time, "%Y年%m月").strftime("%Y-%m")
                                    basic_ent[f'{index_type}_{subject}'].append(ym)
                                except:
                                    try: # 用户给到某年 模糊
                                        nianbao = datetime.strptime(e_time, "%Y年").strftime("%Y")
                                        if f'{nianbao}-12-31' in sj_time:
                                            basic_ent[f'{index_type}_{subject}'].append(f'{nianbao}-12-31')
                                        else:
                                            e_time_trans.append(nianbao)
                                    except:
                                        e_time_trans.append(e_time) 
                                        basic_ent[f'{index_type}_{subject}'].append(e_time) # 模糊
                            if sj_time == []:
                                for tm in e_time_trans:
                                    basic_ent[f'{index_type}_{subject}'].append(tm)
                            if e_time_trans:
                                match_helper(e_time_trans, f'{index_type}_{subject}')

        find_time('财务指标_发布时间', times_fin)
        find_time('十大股东_发布时间', times_gudong)

        # if len(extraction_time) == 0 or extraction_time[0] == '':
        #     for subject, sj_time in times_fin.items():
        #         if sj_time:
        #             basic_ent[f'财务指标_发布时间_{subject}'] = heapq.nlargest(3, sj_time)
        #         else:
        #             basic_ent[f'财务指标_发布时间_{subject}'] = ['2022', '2023', ''] # heapq.nlargest(3, sj_time)
        # else:
        #     for subject, sj_time in times_fin.items():
        #         self.knowledge[f'财务指标_发布时间_{subject}'] = sj_time
        #         for e_time in extraction_time: # 保证用户想要的时间是 图谱中有该主体最接近的时间
        #             if e_time:
        #                 e_time_trans = [] # 精确查询
        #                 try: # 用户给到某年某月某日  精确
        #                     ymd = datetime.strptime(e_time, "%Y年%m月%d日").strftime("%Y-%m-%d")
        #                     e_time_trans.append(ymd)
        #                     if ymd.endswith('12-31') and ymd in sj_time:
        #                         basic_ent[f'财务指标_发布时间_{subject}'].append(ymd) # 精确
        #                     else:
        #                         e_time_trans.append(ymd) 

        #                 except:
        #                     try: # 用户给到某年某月 模糊
        #                         ym = datetime.strptime(e_time, "%Y年%m月").strftime("%Y-%m")
        #                         basic_ent[f'财务指标_发布时间_{subject}'].append(ym)
        #                     except:
        #                         try: # 用户给到某年 模糊
        #                             nianbao = datetime.strptime(e_time, "%Y年").strftime("%Y")
        #                             if f'{nianbao}-12-31' in sj_time:
        #                                 basic_ent[f'财务指标_发布时间_{subject}'].append(f'{nianbao}-12-31')
        #                             else:
        #                                 e_time_trans.append(nianbao)
        #                         except:
        #                             e_time_trans.append(e_time) 
        #                             basic_ent[f'财务指标_发布时间_{subject}'].append(e_time) # 模糊
        #                 if sj_time == []:
        #                     for tm in e_time_trans:
        #                         basic_ent[f'财务指标_发布时间_{subject}'].append(tm)
        #                 if e_time_trans:
        #                     match_helper(e_time_trans, f'财务指标_发布时间_{subject}')
        
        # 用户意图  找出最相关的几个意图作为属性条件
        intent_match = set()
        extraction_intent = ent_dict.get('intent', [])
        for e_intent in extraction_intent:  
            for key in ['属性', '关系', '实体']:
                if key == '关系':  # 关系匹配需要准确 否则查询出来很多 故给定0.5的阈值
                    tmp_scores_bleu = {kg_attr: bleu(kg_attr, e_intent) for kg_attr in self.knowledge[key]}
                    topk = heapq.nlargest(1, tmp_scores_bleu.items(), key=lambda x: x[1])
                else:
                    tmp_scores_ed = {kg_attr: editing_distance(kg_attr, e_intent) for kg_attr in self.knowledge[key]}
                    topk = heapq.nsmallest(self.most_k_similar, tmp_scores_ed.items(), key=lambda x: x[1])
                if (key != '关系' and topk[0][1] == 0) or (key == '关系' and topk[0][1] == 1):
                    intent_match.add(topk[0][0])
                    break
                else:
                    intent_match.update({kg_attr for kg_attr, score in topk if kg_attr not in ['name'] and \
                        score<len(e_intent) and score>0.5}) # not in ['name', '个股']
                    
        # if '股东' in intent_match: # 修改时间  麻烦 todo
        #     for subject, sj_time in times_fin.items():
        #         if sj_time:
        #             basic_ent[f'财务指标_发布时间_{subject}'] = heapq.nlargest(3, sj_time)

        # 单节点查询：意图为单一结点的属性 直接返回该节点信息 并剔除查询该节点类型的意图
        intent_match_tmp = deepcopy(intent_match)
        intent_match_tmp -= {'个股'} # 个股既是实体 也是属性 需要排除
        single_ent = []
        for key, vals in self.knowledge['单节点属性'].items():
            rm_val = vals & intent_match_tmp # 交集
            if rm_val:
                single_ent.append(key)     # 并集保存 查询节点类型
                intent_match_tmp -= rm_val # 剔除该意图
                intent_match -= rm_val
                if key in intent_match:
                    intent_match_tmp -= {key}    # 剔除该节点类型意图
                    intent_match -= {key}    # 剔除该节点类型意图
        
        # 按关系选择路径
        single_path = []
        rels = self.knowledge['关系三元组'].keys()
        rel_rm = rels & intent_match
        if rel_rm:
            for rel in rel_rm:
                single_path.append(rel)     # 并集保存 查询节点类型
            intent_match -= rel_rm # 剔除该意图
                        
        # 此查询会重复查询公共路径 速度慢
        # for subject_type in ('个股', '行业'): 
        #     for subject in set(basic_ent.get(subject_type)):
        #         for time in basic_ent.get(f'财务指标_发布时间_{subject}'):
        #             for ent_type in single_ent: # 单节点查询 返回节点类型 节点属性
        #                 sql_dict['node'].append([f'{subject}在{time}的信息如下\n', f"match path=(n)-[*1..3]->(m:`{ent_type}`) where n.name='{subject}' and \
        #                                             all(node in nodes(path) where (node.发布时间 is null or node.发布时间=~'{time}.*')) \
        #                                             return distinct labels(m)[0], properties(m) LIMIT 1"])
        #             for rel in single_path:
        #                 for _, tail in self.knowledge['关系三元组'][rel]:
        #                     if rel == '基本面':
        #                         for tail2 in self.knowledge['关系三元组辅助'][tail]:
        #                             sql_dict['node'].append([f'{subject}在{time}的信息如下\n', f"match path=(n)-[*1..3]->(m:`{tail2}`) where n.name='{subject}' and \
        #                                                         all(node in nodes(path) where (node.发布时间 is null or node.发布时间=~'{time}.*')) \
        #                                                         return distinct labels(m)[0], properties(m) LIMIT 1"])
        #                     else:
        #                         sql_dict['node'].append([f'{subject}在{time}的信息如下\n', f"match path=(n)-[*1..3]->(m:`{tail}`) where n.name='{subject}' and \
        #                             all(node in nodes(path) where (node.发布时间 is null or node.发布时间=~'{time}.*')) \
        #                             return distinct labels(m)[0], properties(m) LIMIT 1"])

        #             for intent in intent_match:
        #                 sql_dict['node'].append([f'{subject}在{time}的信息如下\n', f"match path=(n)-[*1..3]->(m:`{intent}`) where n.name='{subject}' and \
        #                                             all(node in nodes(path) where (node.发布时间 is null or node.发布时间=~'{time}.*')) \
        #                                             return distinct labels(m)[0], properties(m) LIMIT 1"])
        # return sql_dict

        def help(time):
            '''行业主体不限定时间 规定最多返回三条数据'''
            return ' limit 3' if time == '' else ''

        # 根据将主体作为首节点 时间和用户意图作为条件进行路径查询
        for subject_type in ('个股', '行业'):
            for subject in set(basic_ent.get(subject_type, [])):
                time_set = set()
                
                for time in basic_ent.get(f'财务指标_发布时间_{subject}'):
                    if time in time_set:
                        continue
                    time_set.add(time)
                    
                    for ent_type in single_ent: # 单节点查询 返回节点类型 节点属性
                        if '股东' in ent_type:
                            for time_gd in basic_ent.get(f'十大股东_发布时间_{subject}'):
                                if time_gd in time_set:
                                    continue
                                time_set.add(time_gd)
                                sql_dict['node'].append([f'{subject}{time_gd}的信息如下\n', f"match (n:`{ent_type}`) WHERE (n.name='{subject}' or \
                                                                n.个股='{subject}') AND (n.发布时间 is null or n.发布时间=~'{time_gd}.*') \
                                                            return distinct labels(n)[0], properties(n) limit 5"])
                        else:
                            sql_dict['node'].append([f'{subject}{time}的信息如下\n', f"match (n:`{ent_type}`) WHERE (n.name='{subject}' or \
                                                            n.个股='{subject}') AND (n.发布时间 is null or n.发布时间=~'{time}.*') \
                                                        return distinct labels(n)[0], properties(n) limit 5"])
                        
                    for rel in single_path: # 按关系选择路径 限制时间
                        if rel == '评价':
                            sql_dict['path'].insert(0, [f'{subject}{time}的信息如下\n', f"match path=(n)-[s]-(p)-[r:`评价`]-(m) WHERE n.name='{subject}' and \
                                                            (m.发布时间 IS NULL OR m.发布时间=~'{time}.*') \
                                                          WITH DISTINCT path LIMIT {self.max_return} \
                                                          unwind nodes(path) as node unwind relationships(path) as rel \
                                                          return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)]) # limit {self.max_return}
                        elif rel == '控股':
                            sql_dict['path'].insert(0, [f'{subject}{time}的信息如下\n', f"match path=(n)-[r:`控股`]-(m) WHERE n.name='{subject}' and \
                                    (m.发布时间 IS NULL OR m.发布时间=~'{time}.*') \
                                    WITH DISTINCT path LIMIT {self.max_return} \
                                    unwind nodes(path) as node unwind relationships(path) as rel \
                                    return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)]) # limit {self.max_return}

                        else:
                            sql_dict['path'].insert(0, [f'{subject}{time}的{rel}信息如下\n', f"match path=(n)-[r:`{rel}`]->(m) WHERE n.name='{subject}' and \
                                                            (m.发布时间 IS NULL OR m.发布时间=~'{time}.*') \
                                                          WITH DISTINCT path LIMIT {self.max_return} \
                                                          unwind nodes(path) as node unwind relationships(path) as rel \
                                                          return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)]) # limit {self.max_return}

                            sql_dict['path'].append([f'{subject}{time}的{rel}信息如下\n', f"match path=(n)-[r:`{rel}`]->(m)-[*0..2]-() WHERE (m.发布时间 IS NULL OR m.发布时间=~'{time}.*') and \
                                                        any(node IN nodes(path) where node.name='{subject}') \
                                                        WITH DISTINCT path LIMIT {self.max_return} \
                                                        unwind nodes(path) as node unwind relationships(path) as rel \
                                                        return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)]) # 
                    if intent_match:
                        # 查询主体的2跳路径 是否包含意图中所表示的其中一个属性 
                        # sql_dict['path'].append( f"match path=(n)-[*1..2]-() where n.name='{subject}' \
                        #                         WITH path, reduce(check = false, prop IN {list(intent_match)}| check OR \
                        #                             ANY(node IN nodes(path) WHERE prop IN keys(node) AND node.发布时间=~'{time}.*')) \
                        #                                 AS has_property  WHERE has_property \
                        #                         WITH nodes(path) AS nodes UNWIND nodes AS node \
                        #                         RETURN distinct labels(node) AS labels, properties(node) AS properties")
                        
                        # sql_dict['path'].append( f"match path=(n)-[*1..2]-() where n.name='{subject}' \
                        #                             WITH path, reduce(check = false, prop IN {list(intent_match)}| check OR \
                        #                                 ANY(node IN nodes(path) WHERE prop IN keys(node) AND node.发布时间=~'{time}.*')) \
                        #                                     AS has_property  WHERE has_property \
                        #                             unwind nodes(path) as node unwind relationships(path) as rel \
                        #                             return distinct type(rel), properties(rel), labels(node)[0], properties(node)")

                        # 查询主体与意图实体的2跳路径
                        for intent in intent_match:
                            if intent == '个股':
                                sql_dict['path'].append([f'{subject}{time}的信息如下\n', f"match path=(n:`{subject_type}`)-[]-(m:`{intent}`) where n.name='{subject}' and \
                                                            all(node in nodes(path) where (node.发布时间 is null or node.发布时间=~'{time}.*')) \
                                                            WITH DISTINCT path LIMIT {self.max_return} \
                                                            unwind nodes(path) as node unwind relationships(path) as rel \
                                                            return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)])

                            else:
                                # 查询是否存在该条路径 返回关系类型、关系属性、节点类型、节点属性   
                                sql_dict['path'].append([f'{subject}{time}的信息如下\n', f"match path=(n:`{subject_type}`)-[*1..2]-(m:`{intent}`)-[*0..1]-() where n.name='{subject}' and \
                                                            all(node in nodes(path)[1..3] where (node.发布时间 is null or node.发布时间=~'{time}.*')) \
                                                            WITH DISTINCT path LIMIT {self.max_return} \
                                                            unwind nodes(path) as node unwind relationships(path) as rel \
                                                            return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)])
                                # sql_dict['path'].append( f"match path=(n:`{subject_type}`)-[*1..2]-(m:`{intent}`)-[r]-(p) where n.name='{subject}' and \
                                #                             any(node IN nodes(path) where node.发布时间=~'{time}.*') \
                                #                             unwind nodes(path) as node unwind relationships(path) as rel \
                                #                             return distinct type(r), properties(r), labels(p)[0], properties(p)")
            
                for rel in single_path: # 按关系选择路径 不限制时间 有限制时间的查询结果将优先展示
                    sql_dict['path'].append([f'{subject}的{rel}信息如下\n', f"match path=(n)-[r:`{rel}`]->(m)-[*0..2]-() WHERE \
                                                any(node IN nodes(path) where node.name='{subject}') \
                                                WITH DISTINCT path LIMIT {self.max_return_2} \
                                                unwind nodes(path) as node unwind relationships(path) as rel \
                                                return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)]) #  limit {self.max_return}

        if not sql_dict: # 没有意图直接返回从主体出发的相关信息
            for subject_type in ('个股', '行业'):
                for subject in set(basic_ent.get(subject_type, [])):
                    for time in basic_ent.get(f'财务指标_发布时间_{subject}', []):
                            sql_dict['path'].insert(0, [f'{subject}{time}的信息如下\n', f"match path=(n)-[*]-(m) WHERE n.name='{subject}' and \
                                                            all(node in nodes(path) where node.发布时间 IS NULL OR node.发布时间=~'{time}.*') \
                                                          WITH DISTINCT path LIMIT {self.max_return} \
                                                          unwind nodes(path) as node unwind relationships(path) as rel \
                                                          return distinct type(rel), properties(rel), labels(node)[0], properties(node)"+help(time)]) # limit {self.max_return}
        return sql_dict  
                    
