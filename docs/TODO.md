# TODO
* 基本框架，接受轮询请求，对单个蹲饼器，返回 updated ttl
* (测试用) 蹲饼器，可发请求
* 蹲饼器注册框架，对多个蹲饼器，分别管理状态
* 更新蹲饼器策略, 根据当前各蹲饼器状态，重新分配蹲饼方法.(这里和蹲饼器对齐如何分配.)


* https://github.com/Enraged-Dun-Cookie-Development-Team/cookie-fetcher/tree/master/src/extend
config 对应json5的配置，要保留修改空间.  
* mysql里有interval 是单独配置的；其他字段是config里面序列化了.
* 直接内存.
* 如果warning了就去数据库取config.
* 2022-12-23 10:15:14
    - 引入数据库组件；暂不引入ORM.
    - 写一个get的接口，只要被请求了，就更新结果.
 