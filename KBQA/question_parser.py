import collections
from datetime import datetime
import math
from itertools import combinations
import heapq


# bleu 和 编辑距离 用于计算相似度
def bleu(ner, ent):
    """计算抽取实体与现有实体的匹配度 ner候选词,ent查询词"""
    len_pred, len_label = len(ner), len(ent)
    k = min(len_pred, len_label)
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

    

special_list = [
    ("$", "\$"),
    ("(", "\("),
    (")", "\)"),
    ("*", "\*"),
    ("+", "\+"),
    (".", "\."),
    ("[", "\["),
    ("?", "\?"),
    # "\\",
    ("^", "\^"),
    ("{", "\{"),
    ("|", "\|"),
]


def get_re(s):
    # return s

    for special in special_list:
        s = s.replace(special[0], special[1])
    if len(s) <= 3:
        s = ".*" + ".*".join(list(s)) + ".*"
        for special in special_list:
            s = s.replace(f"\.*{special[0]}", special[1])
        return s
    else:
        return ".*" + s + ".*"


class QuestionParser:
    def __init__(self, knowledge):
        self.most_k_similar = 5
        self.knowledge = knowledge
        
    def question2sql(self, question, ent_dict):
        
        sqls_dict = {} # sql: 结构化输出模板
        sqls_list = [] # 直接返回sql 进行查询

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
                scores_cur = editing_distance(kg_subject, match_subject)
                if scores_cur < scores_best:
                    if flag:
                        basic_ent[subject].append(kg_subject)
                        flag = False
                    else:
                        basic_ent[subject][-1] = kg_subject 
                    scores_best = scores_cur

        #　抽取词 ---> KG中的主体（股票， 行业）和发布时间
        extraction_stuck = ent_dict['主体'].get('股票', [])
        extraction_industry = ent_dict['主体'].get('行业', [])
        if extraction_stuck: # 选出最匹配的个股主体
            for e_stuck in extraction_stuck:
                match_helper([e_stuck], '个股')
          
        if extraction_industry: # 选出最匹配的行业主体
            for e_industry in extraction_industry:
                match_helper([e_industry], '行业')

        # 选出最匹配的时间 为空则使用近一年的
        extraction_time = ent_dict.get('发布时间')
        if len(extraction_time) != 6:
            for e_time in extraction_time:
                if e_time:
                    e_time_trans = [e_time, ]
                    try:
                        e_time_trans.append(datetime.strptime(e_time, "%Y年%m月%d日").strftime("%Y-%m-%d"))
                    except:
                        try:
                            nianbao = datetime.strptime(e_time, "%Y年").strftime("%Y")
                            e_time_trans.append(f'{nianbao}-12-31')
                        except:
                            pass
                    match_helper(e_time_trans, '财务指标_发布时间')
        if not basic_ent['财务指标_发布时间']:
            basic_ent['财务指标_发布时间'] = self.knowledge['近一年']
            
        # 用户意图  找出最相关的几个意图作为属性条件
        extraction_intent = ent_dict.get('intent', [])
        intent_match = collections.defaultdict(set)
        for e_intent in extraction_intent:
            tmp_scores = {kg_attr: editing_distance(kg_attr, e_intent) for kg_attr in self.knowledge['属性']}
            topk = heapq.nsmallest(self.most_k_similar, tmp_scores.items(), key=lambda x: x[1])
            intent_match['属性'].update({kg_attr for kg_attr, _ in topk if kg_attr not in ['name', '个股']})
            
            tmp_scores = {kg_attr: editing_distance(kg_attr, e_intent) for kg_attr in self.knowledge['关系']}
            topk = heapq.nsmallest(self.most_k_similar, tmp_scores.items(), key=lambda x: x[1])
            intent_match['关系'].update({kg_attr for kg_attr, _ in topk})

        # 意图为单一结点的属性 直接返回该节点信息 并剔除查询该节点类型的意图
        single_ent = []
        for key, vals in self.knowledge['单节点属性']:
            rm_val = vals & intent_match['属性'] # 交集
            if rm_val:
                single_ent.append(key)     # 并集保存 查询节点类型
                intent_match['属性'] -= rm_val # 剔除该意图
                intent_match['属性'] -= key    # 剔除该节点类型意图
        
        # 按关系选择路径
        rels = self.knowledge['关系三元组'].keys()
        
        for rel, ht in self.knowledge['关系三元组'].items():
            h, t = ht
            

        
        # 根据将主体作为首节点 时间和用户意图作为条件进行路径查询
        for subject in set(basic_ent.get('个股', []) + basic_ent.get('行业', [])):
            for time in basic_ent.get('财务指标_发布时间'):
                for ent_type in single_ent: # 单节点查询
                    sqls_list.append(f"MATCH (n:`{ent_type}`) WHERE n.name='{subject}' AND n.发布时间='{time}'")
                    
                sqls_list.append( f"MATCH path=(n)-[*1..2]-() where n.name='{subject}' \
                                    WITH path, reduce(check = false, prop IN {list(intent_match)}| check OR \
                                        ANY(node IN nodes(path) WHERE prop IN keys(node) AND node.发布时间='{time}')) \
                                            AS has_property  WHERE has_property \
                                    WITH nodes(path) AS nodes UNWIND nodes AS node \
                                    RETURN distinct labels(node) AS labels, properties(node) AS properties")
        return sqls_list
                    
        # （个股，发布时间） --》 财务指标
        # if basic_ent:
        #     subject, time = basic_ent['主体'], basic_ent['发布时间']

        #     # 用户意图：财务指标
        #     if '财务指标' in ent_dict.values():
        #         sqls_dict[f"match (m:`财务指标`)-[]-(n) where n.name='{stuck}' and n.发布时间='{time}' return properties(n)"] = \
        #             f"{ent_dict['个股']}在{ent_dict['发布时间']}的财务指标包括："
            
        #     # 用户意图：其中的一项财务指标
        #     fin_idx = ent_dict['财务指标']
        #     scores_best = float('inf')
        #     for fin_ent in ["每股指标" , "财务风险", "常用指标", "营运能力", "成长能力", "收益质量", '盈利能力']:
        #         if fin_ent == fin_idx:
        #             basic_ent['具体财务指标'] = fin_ent
        #             break
        #         scores_now = editing_distance(fin_idx, fin_ent)
        #         if scores_now < scores_best:
        #             basic_ent['具体财务指标'] = fin_ent
        #             scores_best = scores_now
        #     fin_ent = basic_ent['具体财务指标']
        #     if fin_ent:
        #         sqls_dict[f"match (n:`{fin_ent}`) where n.name='{stuck}' and n.发布时间='{time}' return properties(n)"] = \
        #             f"properties(n) name,发布时间 {ent_dict['个股']}在{ent_dict['发布时间']}的{fin_ent}："

        #     # 用户意图：具体的一个财务指标
        #     extraction_attr = ent_dict.get('ner', None)
        #     if extraction_attr:
        #         scores_best = float('inf')
        #         for kg_attr in self.knowledge['属性']:
        #             if kg_attr == extraction_attr:
        #                 basic_ent['属性'] = kg_attr
        #                 break
        #             scores_now = editing_distance(kg_attr, extraction_attr)
        #             if scores_now < scores_best:
        #                 basic_ent['属性'] = kg_attr
        #                 scores_best = scores_now
        #     ent_attr = basic_ent['属性']
        #     if ent_attr:
        #         sqls_dict[f"match (m:`财务指标`)-[]-(n) where n.name='{stuck}' and n.发布时间='{time}' return n.{ent_attr}"] = \
        #             f"n.{ent_attr} name,发布时间 {ent_dict['个股']}在{ent_dict['发布时间']}的{ent_attr}："    
        
        return sqls_dict
        