import boto3
from datetime import date, datetime, timedelta
import os

db_id     = os.environ["DB_ID"]
s3_bucket = os.environ["S3_BUCKET"]

def lambda_handler(event, context):
    # 1時間前の時刻を取得（UTC）→対象時間のPerformance InsightsからS3へ
    lasthour = (datetime.today() + timedelta(hours = -1)).replace(minute=0, second=0, microsecond=0)

    print("Export: " + (lasthour + timedelta(hours = 9)).strftime('%Y/%m/%d %H:00-%H:59'))
    # 1分ごとに上位最大10件の正規化SQLを取得してS3へ転記
    for minute in range(60):
        # Performance Insightsからデータを取得
        pi_client = boto3.client("pi")
        starttime = lasthour + timedelta(minutes = minute)
        response = pi_client.describe_dimension_keys(
                ServiceType="RDS",
                Identifier=db_id,
                StartTime=starttime,
                EndTime=starttime + timedelta(minutes = 1),
                Metric="db.load.avg",
                PeriodInSeconds=60,
                GroupBy={
                    "Group": "db.sql_tokenized",
                    "Dimensions": [
                        "db.sql_tokenized.statement"
                    ],
                    "Limit": 10
                    }
                )
        if len(response["Keys"]):
            # 対象となる時刻（分単位）のデータがあればS3へ転記（プレフィクスの時刻はJST）
            s3_prefix  = db_id + "/" + (starttime + timedelta(hours = 9)).strftime('%Y/%m/%d/%Y%m%d%H%M') + "_" + db_id + ".tsv"
            exporttime = starttime.strftime('%Y-%m-%dT%H:%M:%SZ')
            body_data  = "start_time\tsql_tokenized\ttotal\n"
            # すべてのKeysから正規化SQLと合計値を抽出
            for item in response["Keys"]:
                sqltk = item["Dimensions"]["db.sql_tokenized.statement"]
                total = item["Total"]
                body_data += exporttime + "\t" + sqltk + "\t" + str(total) + "\n"
            # S3へ
            s3_client = boto3.client("s3")
            s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_prefix,
                    Body=body_data
                    )
    return "Completed."
