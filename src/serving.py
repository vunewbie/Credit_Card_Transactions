from io import StringIO
from hdfs import InsecureClient
from dotenv import load_dotenv
import os, re, requests
import pandas as pd

load_dotenv()

HDFS_WEB_URL = os.getenv("HDFS_WEB_URL")
HDFS_USER = os.getenv("HDFS_USER")
HDFS_INTERNAL_PATH = os.getenv("HDFS_INTERNAL_PATH")
POWER_BI_URL = os.getenv("POWER_BI_URL")

def read_last_push_timestamp(file_path):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        print("Đây là lần đầu push dữ liệu lên Power BI, không có timestamp cũ.")
        return None

    try:
        with open(file_path, "r") as f:
            timestamp_str = f.read().strip()
        
        last_ts = pd.to_datetime(timestamp_str)
        print(f"Đã đọc timestamp cũ: {last_ts}")
        
        return last_ts
    except Exception as e:
        print(f"Lỗi khi đọc timestamp: {e}")
        return None

def read_hadoop_latest_data(last_push_ts):
    client = InsecureClient(HDFS_WEB_URL, user=HDFS_USER)
    files = client.list(HDFS_INTERNAL_PATH)
    part_files = [f for f in files if re.match(r'part-.*', f)]

    final_df = pd.DataFrame()

    for file_name in part_files:
        file_path = f"{HDFS_INTERNAL_PATH.rstrip('/')}/{file_name}"

        with client.read(file_path, encoding='utf-8') as reader:
            csv_data = reader.read()

        if not csv_data.strip():
            continue

        try:
            df = pd.read_csv(StringIO(csv_data), parse_dates=["Timestamp"], dayfirst=True)
        except Exception as e:
            print(f"Lỗi đọc file {file_name}: {e}")
            continue

        if last_push_ts is None:
            if not df.empty:            
                final_df = pd.concat([final_df, df], ignore_index=True)
        else:
            df_filtered = df[df["Timestamp"] > last_push_ts]
            
            if not df_filtered.empty:
                final_df = pd.concat([final_df, df_filtered], ignore_index=True)

    return final_df

def push_data_to_powerbi(df, push_url, batch_size=5000):
    if df.empty:
        print("DataFrame trống, không có dữ liệu để đẩy.")
        return False

    if "Timestamp" in df.columns:
        df = df.drop(columns=["Timestamp"])

    df = df.fillna("")
    df = df.infer_objects(copy=False)
    records = df.to_dict(orient="records")
    total = len(records)
    headers = {
        "Content-Type": "application/json"
    }

    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        payload = {"rows": batch}
        response = requests.post(push_url, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"Đã push {len(batch)} dòng thành công (batch {i}-{i+len(batch)-1})")
        else:
            print(f"Lỗi khi push batch {i}-{i+len(batch)-1}: {response.status_code} {response.text}")
            return False

    return True

def update_last_push_timestamp(df, output_path):
    if "Timestamp" not in df.columns or df.empty:
        print("Không thể cập nhật timestamp.")
        return

    latest_ts = df["Timestamp"].max()
    
    with open(output_path, "w") as f:
        f.write(latest_ts.strftime("%Y-%m-%d %H:%M:%S"))
    print(f"Đã cập nhật timestamp mới: {latest_ts}")

def main():
    file_path = os.path.join(os.getenv("HOME"), "ODAP", "src", "last_push_timestamp.txt")
    last_push_ts = read_last_push_timestamp(file_path)
    df = read_hadoop_latest_data(last_push_ts)

    if df.empty:
        print("Không có dữ liệu mới để push.")
    else:
        print(f"Đã lọc được {len(df)} dòng dữ liệu mới:")
        print(df.head())

    if POWER_BI_URL:
        push_data_to_powerbi(df, POWER_BI_URL)
    else:
        print("Không tìm thấy biến môi trường POWER_BI_URL.")

    update_last_push_timestamp(df, file_path)
    print("Quá trình hoàn tất.")

if __name__ == "__main__":
    main()
