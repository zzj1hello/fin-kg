
1. 使用线程控制查询超时，这部分主要是无结果的路径查找，该方法和直接使用第三方库做函数装饰器，效果相同，但都无法解决，一个线程持续查询，得等到主程序结束，才销毁的问题
   - 可能得寻求Neo4j的配置，限制查询时间
        def run_query(query, queue, stop_event):
            result = self.g.run(query)
            try:
                result = self.g.run(query)
            except Exception as e:
                print("查询出错:", e)
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
            timeout_seconds = 5 # 设置查询超时时间（以秒为单位）
            thread.join(timeout_seconds) # 等待线程执行完毕或超时
            if thread.is_alive(): # 判断线程是否仍在运行
                print("查询超时")
                stop_event.set() 
                # thread.join()
                res = None
            else:
                res = result_queue.get()
            return res

        @timeout_decorator.timeout(5)
        def test(query):
            try:
                result = self.g.run(query)
                return result
            except Exception as e:
                print("查询出错:", e)
                return None
            finally:
                pass


- 删除索引
1. 先找到索引名 SHOW RANGE INDEXES WHERE owningConstraint IS NULL
2. drop index 索引名

- 评价 查询失败
- 2个股 2年份 查询截断了一个个股的一个年份
- 目前在30个问题上的查询速度和返回字符数  ![](./test-qa.png)