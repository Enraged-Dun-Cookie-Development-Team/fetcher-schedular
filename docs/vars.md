# 变量说明
本文档对当前项目中，多次出现的重要变量的统一规定，进行记录与说明。

* hour_index
  - 代表现实世界时间的小时数，例如hour_index=20 代表晚间20时；而非相对于 ```AUTO_SCHE_CONFIG['DAILY_PREPROCESS_TIME']['HOUR']``` 的偏移量。