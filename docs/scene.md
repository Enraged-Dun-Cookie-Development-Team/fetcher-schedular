# 典型场景记录

## 新增一个蹲饼器
* 蹲饼器调用注册接口，maintainer保存last_updated_time
* 过一段较短的时间，health_monitor定期任务发现蹲饼器有效数量变化(通过last_updated_time计算得到)
    * 触发fetcher_config_pool的更新(根据已有的ALIVE状态的 instance_id 的列表，得到当前每个fetcher 的config)
    * 触发maintainer的更新，把相关的 instance_id 的need_update状态改为True
* 过一段较短的时间，每个蹲饼器传来请求，更新自己的config，need_update分别改为False.

## 某个蹲饼器心跳无响应
* 过一段较短的时间，health_monitor定期任务发现蹲饼器有效数量变化(通过last_updated_time计算得到)
    * 触发fetcher_config_pool的更新(根据已有的ALIVE状态的 instance_id 的列表，得到当前每个fetcher 的config)
    * 触发maintainer的更新，把相关的 instance_id 的need_update状态改为True
* 过一段较短的时间，每个蹲饼器传来请求，更新自己的config，need_update分别改为False.

## 来自后台的config更新
* 后端调用更新接口，此时默认蹲饼器的ALIVE数量是不变的
    * 触发fetcher_config_pool的更新
    * 触发maintainer的更新，把相关的 instance_id 的need_update状态改为True
* 过一段较短的时间，每个蹲饼器传来请求，更新自己的config，need_update分别改为False