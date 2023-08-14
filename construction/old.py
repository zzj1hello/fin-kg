

for rel_name in set(df_data['分类']):
    data_val = df_data[df_data['分类']==rel_name].iloc[:, 2:].values
    father_name = ''
    for d in data_val:
        name, vals = escape_string(d[0]), d[1:]
        node_properties = dict(zip(attr_names, vals))
        
        if name in ('合计', '其他', '小计', '其他业务(补充)'): # 特别划分 "合计" 的关系
            name = rel_name[:-1] + name if '计' in name else name[:2] + rel_name[1:-1] + name[2:]
            
        # 特别划分 "其中:" 的关系
        child_name = re.findall(r'其中：(.*)', name)
        if len(child_name)>0:
            name = child_name[0]
        else:
            father_name = name
            
        # 创建节点
        node_properties.update({'name': name, '个股': stuck, '发布时间': report_period})
        g.run("create CONSTRAINT IF NOT EXISTS FOR (n: `主营指标`) REQUIRE (n.name, n.个股, n.发布时间) IS Unique")
        try:
            g.create(Node("主营指标",**node_properties))
        except:
            pass
        
        if len(child_name)>0: 
            g.run(f"match (p:`主营指标`), (q:`主营指标`) where p.name='{father_name}' and p.个股='{stuck}' and p.发布时间='{report_period}' \
                    and q.name='{name}' and q.个股='{stuck}' and q.发布时间='{report_period}'  \
                        merge (p)-[r:`包含`]->(q)")
        else:
            g.run(f"match (m:`主营构成`), (p:`主营指标`) where m.name='{stuck}' and m.发布时间='{report_period}'\
                    and p.name='{name}' and p.个股='{stuck}' and p.发布时间='{report_period}'  merge (m)-[r:`{rel_name}`]->(p)")
