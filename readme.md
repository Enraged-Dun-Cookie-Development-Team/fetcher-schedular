# 调度器
# 功能
小刻食堂后端用于对各蹲饼器进行调度的服务。

调度器发挥作用的场景请参考[典型场景记录](docs/scene.md)

# 运行方式

施工中，项目采用[tornado](https://www.tornadoweb.org/en/stable/)框架。
通过在项目根目录

``pip install -r requirement.txt`` 安装依赖项；

开发时，于[db.py](src/db.py)中修改相关数据库连接的配置。(稍后改为环境变量传入)

``python3 src/schedular.py`` 启动。


# 可用接口

请参考测试脚本 [test_post.py](dev-test/test_post.py) 获知目前可以调通的接口。

# 本地开发

* 配置本地环境变量
* 启动数据库
* 启动redis
* 启动grpc server
* 启动调度器

# 其他说明

* 对于项目中一些变量的统一说明可见[vars.md](docs/vars.md)