import pandas as pd
import os


# 数据路径设置
DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')

# 1. Germany
DATA_PATH = os.path.join(DIR, 'GTFS', 'Germany')
SAVE_PATH = os.path.join(DATA_PATH, 'output')

# 2. Italy
DATA_PATH2 = os.path.join(DIR, 'GTFS', 'Italy')
SAVE_PATH2 = os.path.join(DATA_PATH2, 'output')

# 3. Netherlands
DATA_PATH3 = os.path.join(DIR, 'GTFS', 'Netherlands')
SAVE_PATH3 = os.path.join(DATA_PATH3, 'output')

# 4. Norway
DATA_PATH4 = os.path.join(DIR, 'GTFS', 'Norway')
SAVE_PATH4 = os.path.join(DATA_PATH4, 'output')

# 5. Poland
DATA_PATH5 = os.path.join(DIR, 'GTFS', 'Poland')
SAVE_PATH5 = os.path.join(DATA_PATH5, 'output')

# 6. Portugal
DATA_PATH6 = os.path.join(DIR, 'GTFS', 'Portugal')
SAVE_PATH6 = os.path.join(DATA_PATH6, 'output')

# 7. Spain
DATA_PATH7 = os.path.join(DIR, 'GTFS', 'Spain')
SAVE_PATH7 = os.path.join(DATA_PATH7, 'output')

# 8. Sweden
DATA_PATH8 = os.path.join(DIR, 'GTFS', 'Sweden')
SAVE_PATH8 = os.path.join(DATA_PATH8, 'output')

# 9. Switzerland
DATA_PATH9 = os.path.join(DIR, 'GTFS', 'Switzerland')
SAVE_PATH9 = os.path.join(DATA_PATH9, 'output')

# 加载 GTFS 数据
def load_gtfs_data(data_path):
    """
    加载 GTFS 数据文件
    """
    stops = pd.read_csv(os.path.join(data_path, 'stops.txt'),low_memory=False, dtype=str)
    routes = pd.read_csv(os.path.join(data_path, 'routes.txt'),low_memory=False, dtype=str)
    stop_times = pd.read_csv(os.path.join(data_path, 'stop_times.txt'),low_memory=False, dtype=str)
    trips = pd.read_csv(os.path.join(data_path, 'trips.txt'),low_memory=False, dtype=str)
    return stops, routes, stop_times, trips


# 提取地铁站点信息
def extract_metro_stations(stops, routes, stop_times, trips):
    """
    提取地铁相关站点，包括终点站和换乘站
    """
    # 筛选地铁线路 (route_type: 地铁通常为 1)
    metro_routes = routes[routes["route_type"] == "1"]
    metro_trips = trips[trips["route_id"].isin(metro_routes["route_id"])]
    metro_stop_times = stop_times[stop_times["trip_id"].isin(metro_trips["trip_id"])]

    # 提取地铁站点
    metro_stops = stops[stops["stop_id"].isin(metro_stop_times["stop_id"])]

    # 识别终点站（每条线路的最大 stop_sequence）
    end_stations = metro_stop_times.loc[metro_stop_times.groupby("trip_id")["stop_sequence"].idxmax()]
    end_station_ids = end_stations["stop_id"].unique()
    end_stops = metro_stops[metro_stops["stop_id"].isin(end_station_ids)]

    # 识别换乘站（通过 stop_id 是否出现在多个 trip_id 中）
    transfer_stops = metro_stop_times.groupby("stop_id").filter(lambda x: x["trip_id"].nunique() > 1)
    transfer_stop_ids = transfer_stops["stop_id"].unique()
    transfer_stops = metro_stops[metro_stops["stop_id"].isin(transfer_stop_ids)]

    # 添加属性字段
    metro_stops["whetherTerminal"] = metro_stops["stop_id"].isin(end_stops["stop_id"]).astype(int)
    metro_stops["transStation"] = metro_stops["stop_id"].isin(transfer_stops["stop_id"]).astype(int)

    # 仅仅保留 stop_name,whetherTerminal,transStation,stop_lat,stop_lon
    metro_stops = metro_stops[["stop_id", "stop_name", "whetherTerminal", "transStation", "stop_lat", "stop_lon"]]

    return metro_stops, end_stops, transfer_stops


# 提取地铁站点信息
def extract_metro_stations_for_Switzerland(stops, routes, stop_times, trips):
    """
    提取地铁相关站点，包括终点站和换乘站
    """
    # 筛选地铁线路 (route_type: 地铁通常为 1)
    metro_routes = routes[routes["agency_id"] == 55] # 洛桑地铁 瑞士仅有的地铁 仅有两条线路
    metro_trips = trips[trips["route_id"].isin(metro_routes["route_id"])]
    metro_stop_times = stop_times[stop_times["trip_id"].isin(metro_trips["trip_id"])]

    # 提取地铁站点
    metro_stops = stops[stops["stop_id"].isin(metro_stop_times["stop_id"])]

    # 识别终点站（每条线路的最大 stop_sequence）
    end_stations = metro_stop_times.loc[metro_stop_times.groupby("trip_id")["stop_sequence"].idxmax()]
    end_station_ids = end_stations["stop_id"].unique()
    end_stops = metro_stops[metro_stops["stop_id"].isin(end_station_ids)]

    # 识别换乘站（通过 stop_id 是否出现在多个 trip_id 中）
    transfer_stops = metro_stop_times.groupby("stop_id").filter(lambda x: x["trip_id"].nunique() > 1)
    transfer_stop_ids = transfer_stops["stop_id"].unique()
    transfer_stops = metro_stops[metro_stops["stop_id"].isin(transfer_stop_ids)]

    # 添加属性字段
    metro_stops["whetherTerminal"] = metro_stops["stop_id"].isin(end_stops["stop_id"]).astype(int)
    metro_stops["transStation"] = metro_stops["stop_id"].isin(transfer_stops["stop_id"]).astype(int)

    return metro_stops, end_stops, transfer_stops

# 保存结果
def save_results(output_path, metro_stops, end_stops, transfer_stops):
    """
    保存结果到 CSV 文件
    """
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    metro_stops.to_csv(os.path.join(output_path, "metro_stations.csv"), index=False)
    end_stops.to_csv(os.path.join(output_path, "end_stations.csv"), index=False)
    transfer_stops.to_csv(os.path.join(output_path, "transfer_stations.csv"), index=False)
    print(f"结果已保存到: {output_path}")


# 主函数
if __name__ == "__main__":
    # 1. Germany
    # stops, routes, stop_times, trips = load_gtfs_data(DATA_PATH)
    # metro_stops, end_stops, transfer_stops = extract_metro_stations(stops, routes, stop_times, trips)
    # save_results(SAVE_PATH, metro_stops, end_stops, transfer_stops)

    # 2. Switzerland
    stops, routes, stop_times, trips = load_gtfs_data(DATA_PATH2)
    metro_stops, end_stops, transfer_stops = extract_metro_stations(stops, routes, stop_times, trips)
    save_results(SAVE_PATH2, metro_stops, end_stops, transfer_stops)

