# GTFS 
> **The Mobility Database** :
> - The Mobility Database is an international catalog of public transit data for transit agencies, rider-facing apps, technology vendors, researchers, and others to use. It features over 2,000 GTFS and GTFS Realtime feeds, including 500+ feeds unavailable on the old TransitFeeds website.
> - It offers data quality reports from the Canonical GTFS Schedule Validatoraiming to improve data transparency and quality. The platform aspires to become a sustainable, central hub for global mobility data.

从 [Mobility Database*](https://mobilitydatabase.org) 提供的API下载指定城市的GTFS数据集。

## 1. 运行项目
你需要首先安装本项目的依赖包，然后运行项目。

此外你还需要在本地创建一个名为`.env`的文件用于存放你的`REFRESH_TOKEN`。

```bash
REFRESH_TOKEN = "your_refresh_token"
```

首先在api中下载指定城市的GTFS数据集，然后在main.py中运行项目。