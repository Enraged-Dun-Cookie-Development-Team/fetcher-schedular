# 调度器
# 功能
小刻食堂后端用于对各蹲饼器进行调度的服务。

调度器发挥作用的场景请参考[典型场景记录](docs/scene.md)

# 运行方式

施工中，项目采用[tornado](https://www.tornadoweb.org/en/stable/)框架。
通过在项目根目录

``pip install -r requirement.txt`` 安装依赖项；

``python3 src/schedular.py`` 启动。


# 可用接口

请参考测试脚本 [test_post.py](dev-test/test_post.py) 获知目前可以调通的接口。