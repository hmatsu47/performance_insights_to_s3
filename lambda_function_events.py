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
        starttime = lasthour + timedelta(minutes = minute)
        metric    = "db.load.avg"
        periods   = 60
        groupby   = {
                    "Group": "db.sql_tokenized",
                    "Dimensions": [
                        "db.sql_tokenized.statement",
                        "db.sql_tokenized.id"
                    ],
                    "Limit": 10
                    }
        filter    = {}
        response = pi_fetch(db_id, starttime, metric, periods, groupby, filter)
        if len(response["Keys"]):
            # 対象となる時刻（分単位）のデータがあればS3へ転記（プレフィクスの時刻はJST）
            s3_prefix  = db_id + "/" + (starttime + timedelta(hours = 9)).strftime('%Y/%m/%d/%Y%m%d%H%M') + "_" + db_id + ".tsv"
            exporttime = starttime.strftime('%Y-%m-%dT%H:%M:%SZ')
            body_data  = "start_time\tsql_tokenized\ttotal\n"
            event_s3_prefix = db_id + "_wait_events/" + (starttime + timedelta(hours = 9)).strftime('%Y/%m/%d/%Y%m%d%H%M') + "_" + db_id + "_wait_events.tsv"
            event_body_data = "start_time\tsql_tokenized\tevent_type\tevent_name\ttotal\n"
            # すべてのKeysから正規化SQLと合計値を抽出
            for item in response["Keys"]:
                sqltk = item["Dimensions"]["db.sql_tokenized.statement"]
                sqlid = item["Dimensions"]["db.sql_tokenized.id"]
                total = item["Total"]
                body_data += exporttime + "\t" + sqltk + "\t" + str(total) + "\n"
                # 正規化SQL別の待機イベントを取得してS3へ転記
                event_groupby  = {
                                "Group": "db.wait_event",
                                "Dimensions": [
                                "db.wait_event.type",
                                "db.wait_event.name"
                                ],
                                "Limit": 10
                                }
                event_filter   = {
                                "db.sql_tokenized.id": sqlid
                                } 
                event_response = pi_fetch(db_id, starttime, metric, periods, event_groupby, event_filter)
                if len(event_response["Keys"]):
                    # 対象となる時刻（分単位）の待機イベントがあればS3へ転記
                    for event_item in event_response["Keys"]:
                        event_type       = event_item["Dimensions"]["db.wait_event.type"]
                        event_name       = event_item["Dimensions"]["db.wait_event.name"]
                        event_total      = event_item["Total"]
                        event_body_data += exporttime + "\t" + sqltk + "\t" + event_type + "\t" + event_name + "\t" + str(event_total) + "\n"
            # S3へ
            s3_put(s3_bucket, s3_prefix, body_data)
            s3_put(s3_bucket, event_s3_prefix, event_body_data)
    return "Completed."

def pi_fetch(db_id, starttime, metric, periods, groupby, filter):
    pi_client = boto3.client("pi")
    response = pi_client.describe_dimension_keys(
            ServiceType="RDS",
            Identifier=db_id,
            StartTime=starttime,
            EndTime=starttime + timedelta(minutes = 1),
            Metric=metric,
            PeriodInSeconds=periods,
            GroupBy=groupby,
            Filter=filter
            )
    return response

def s3_put(bucket, key, body):
    s3_client = boto3.client("s3")
    s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body
            )