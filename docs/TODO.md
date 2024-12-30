# TODO
* 校验数据点的数量 = 实际的datasource_num * time_points.
    * time_points 是以interval（默认为1秒）控制间隔的。比较容易控制数量
    * 难点是datasource_num
        - 机器学习模型是离线训练的。它学习了固定的 datasource 映射。
        - 需要确保：机器学习模型输入一个原始datasource_id时，会映射变成正确的输入feature.(241230现在有bug) 
        - 在离线训练期间未遇到的datasource，例如新增的，走default逻辑。
 