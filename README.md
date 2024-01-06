## Lab1

### 开发小记

~~worker在正常流程里check服务器是否还在工作/工作是否完成，在额外的goroutinue里keep alive~~

在coordinator端实现worker存活判定，且以新的worker访问为触发时机，并不单独监控workers存活情况「有点像乐观锁🤔️，写入时检查version判脏写」。：由于server监听是单向的，服务器推送（由coordinator向worker发请求）实现成本复杂，所以以worker访问为触发时机。

3⃣️种keep alive的方式：1、服务端轮训；2、客户端轮训，服务端记录访问时间，并固定间隔检查超时情况；3、服务端记录分配时间，客户端访问时检查超时情况；

shuffle部分由reduce继承，具体方法为map阶段输出mr-[mapID]-[reduceID]命名格式的文件，再由reduce worker统一收集（可以优化的点是，再分布式系统中，尝试依照结尾的reduceID「即在map阶段敲定reduce分配，在这里是通过ihash实现的」定向存储文件）

💡

* ihash预分配reduce任务
* json格式存储中间文件
* hasTimeOut判活，无需额外开销

✅

* 完成基本业务需求：实现**分布式**mapreduce
* 通过测试脚本
* 实现基本容错：worker crash，task重分配

❌

* 未catch err
* 未研究测试脚本
* no backup&&rename when completed, cause no TempFile