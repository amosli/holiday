from datetime import datetime, timedelta
import pymysql
import requests


# 数据库连接配置
db_config = {
    "host": "10.0.21.5",
    "port": 3306,
    "user": "user",
    "password": "password",
    "database": "test",
}
JSON_URL_TEMPLATE = "http://chinese-holidays-data.basten.me/data/{year}.json"
# JSON 数据
# holiday_data = [
#     {"name": "元旦", "range": ["2025-01-01"], "type": "holiday"},
#     {"name": "春节", "range": ["2025-01-26"], "type": "workingday"},
#     {"name": "春节", "range": ["2025-01-28", "2025-02-04"], "type": "holiday"},
#     {"name": "春节", "range": ["2025-02-08"], "type": "workingday"},
#     {"name": "清明节", "range": ["2025-04-04", "2025-04-06"], "type": "holiday"},
#     {"name": "劳动节", "range": ["2025-04-27"], "type": "workingday"},
#     {"name": "劳动节", "range": ["2025-05-01", "2025-05-05"], "type": "holiday"},
#     {"name": "端午节", "range": ["2025-05-31", "2025-06-02"], "type": "holiday"},
#     {"name": "国庆节、中秋节", "range": ["2025-09-28"], "type": "workingday"},
#     {"name": "国庆节、中秋节", "range": ["2025-10-01", "2025-10-08"], "type": "holiday"},
#     {"name": "国庆节、中秋节", "range": ["2025-10-11"], "type": "workingday"},
# ]


def create_holiday_table():
    table_sql='''
        CREATE TABLE if not exists non_workdays (
            non_work_date DATE NOT NULL COMMENT '非工作日日期',
            description VARCHAR(255) COMMENT '描述：节假日或周末'
        )
        UNIQUE KEY(non_work_date)
        DISTRIBUTED BY HASH(non_work_date) BUCKETS 2
        PROPERTIES (
            "replication_num" = "3",
            "enable_unique_key_merge_on_write" = "true",
            "storage_format" = "V2"
        );
    '''
    """将非工作日插入到数据库"""
    connection = pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        port=db_config["port"],
        charset="utf8mb4"
    )
    cursor = connection.cursor()

    try:
        # 插入数据
        cursor.execute(table_sql)
        connection.commit()
       
    except Exception as e:
        print(f"Error create table : {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

    

def fetch_holiday_data(year):
    """从指定 URL 获取节假日 JSON 数据"""
    url = JSON_URL_TEMPLATE.format(year=year)
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching holiday data for {year}: {e}")
        return []


def get_all_dates(year):
    """生成指定年份的所有日期"""
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    delta = timedelta(days=1)
    all_dates = []
    while start_date <= end_date:
        all_dates.append(start_date)
        start_date += delta
    return all_dates

def parse_holiday_data(holiday_data):
    """解析节假日和补班数据"""
    holidays = set()
    working_days = set()
    for item in holiday_data:
        name = item["name"]
        date_range = item["range"]
        date_type = item["type"]

        # 处理日期范围
        start_date = datetime.strptime(date_range[0], "%Y-%m-%d")
        end_date = datetime.strptime(date_range[-1], "%Y-%m-%d")
        delta = timedelta(days=1)
        while start_date <= end_date:
            if date_type == "holiday":
                holidays.add((start_date, name))
            elif date_type == "workingday":
                working_days.add(start_date)
            start_date += delta
    return holidays, working_days

def calculate_non_workdays(all_dates, holidays, working_days):
    """计算非工作日（节假日 + 周末 - 补班日）"""
    non_workdays = []
    for date in all_dates:
        is_weekend = date.weekday() in [5, 6]  # 周六、周日
        holiday_name = next((name for d, name in holidays if d == date), None)

        if is_weekend or holiday_name:
            # 如果是补班日，则跳过
            if date in working_days:
                continue
            description = holiday_name if holiday_name else "周末"
            non_workdays.append((date.strftime("%Y-%m-%d"), description))
    return non_workdays

def insert_non_workdays_to_db(non_workdays, db_config, year):
    """将非工作日插入到数据库"""
    connection = pymysql.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        port=db_config["port"],
        charset="utf8mb4"
    )
    cursor = connection.cursor()

    try:
        # 清理旧数据
        delete_sql = "DELETE FROM non_workdays WHERE YEAR(non_work_date) = %s"
        cursor.execute(delete_sql, (year,))
        connection.commit()

        # 插入新数据
        insert_sql = "INSERT INTO non_workdays (non_work_date, description) VALUES (%s, %s)"
        cursor.executemany(insert_sql, non_workdays)
        connection.commit()
        print(f"Successfully inserted {len(non_workdays)} non-workdays for year {year}.")
    except Exception as e:
        print(f"Error inserting data for year {year}: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def process_year(year):
    """处理指定年份的非工作日计算和数据库插入"""
    print(f"Processing year {year}...")
    # 获取节假日数据
    holiday_data = fetch_holiday_data(year)
    if not holiday_data:
        print(f"No data available for year {year}. Skipping...")
        return

    # 解析数据
    holidays, working_days = parse_holiday_data(holiday_data)

    # 获取所有日期
    all_dates = get_all_dates(year)

    # 计算非工作日
    non_workdays = calculate_non_workdays(all_dates, holidays, working_days)

    # 插入到数据库
    insert_non_workdays_to_db(non_workdays, db_config, year)
# 主流程
def main():
    create_holiday_table()
    for year in range(2015,2025):    
        process_year(year)    
   
if __name__ == "__main__":
    main()
