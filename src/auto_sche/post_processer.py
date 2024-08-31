import pandas as pd


def batch_predict(X, clf):
    """
    批量返回蹲饼时间.

    蹲饼策略：
    1. 有饼蹲饼
    2. 没饼则1分钟蹲一次(白天)/1小时蹲一次(深夜)；该规则可进一步细化.
    """
    X = X.copy()

    # 预测是否要蹲饼.
    raw_y = clf.predict(X)

    # hour, minute, second 三列
    # X.iloc[:, -4:-1]

    predict_time = []  # 形状为 (n, 3)
    for idx in range(X.shape[0]):

        hour, minute, second = X.iloc[idx, -4], X.iloc[idx, -3], X.iloc[idx, -2]
        # (预测)有饼则蹲饼
        if raw_y[idx] == 1:
            predict_time.append([hour, minute, second])

        # (预测)没饼则在整分钟(白天)或整小时(深夜)蹲饼
        else:

            # 白天
            if hour >= 8 and hour <= 22:
                second = 0
                minute, minute_carry = (minute + 1) % 60, int(minute == 59)
                hour = (hour + minute_carry) % 24

            # 晚上
            else:
                second, minute = 0, 0
                hour = (hour + 1) % 24

            predict_time.append([hour, minute, second])

    df_time = pd.DataFrame(predict_time)
    df_time.columns = ['pred_hour', 'pred_minute', 'pred_second']
    print(df_time.shape)
    print(X.shape)
    return pd.concat([X.reset_index(drop=True), df_time.reset_index(drop=True)], axis=1)


def time_to_seconds(row):
    try:
        return row['hour'] * 3600 + row['minute'] * 60 + row['second']
    except:
        return row['pred_hour'] * 3600 + row['pred_minute'] * 60 + row['pred_second']


def delay_estimation(predict_time, actual_time, y_label=None):
    """
    计算模型给出的蹲饼时间和实际蹲饼时间的diff
    当给定y_label时，只计算y_label = 1，也就是有饼场合的延迟.

    输入: predict_time 和 actual_time均为 h:m:s
    输出: 延迟秒数.
    """
    y_label, predict_time, actual_time = y_label.reset_index(drop=True), predict_time.reset_index(
        drop=True), actual_time.reset_index(drop=True)

    predict_time = predict_time[y_label == 1]
    actual_time = actual_time[y_label == 1]

    total_seconds_pred = predict_time.apply(time_to_seconds, axis=1)
    total_seconds_actual = actual_time.apply(time_to_seconds, axis=1)

    return (total_seconds_pred - total_seconds_actual + 86400) % 86400
