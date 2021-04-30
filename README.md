# performance_insights_to_s3
 RDS/Aurora Performance Insights to S3 (Tokenized SQL)

## 内容

 - Amazon RDS / Aurora の Performance Insights で記録された Tokenized SQL 分間 TOP 10 を S3 バケットに転記する Python Script です。
 - Lambda で毎時実行すると、1 時間前の時間帯の記録を 1 分単位で集計して指定の S3 バケットに保存します。
 - Athena で扱いやすいように `.tsv` で保存します。

## 設定項目

 - 実行時間 : 1-3 分程度

 - 環境変数 :
   - `DB_ID` : 対象となる RDS / Aurora インスタンスのデータベース ID
   - `S3_BUCKET` : 保存先の S3 バケット名

 - トリガー : CloudWatch Events で cron 定時実行（毎時 5 分など）

## 注意点

 - Lambda のロールには以下のポリシー（権限）が必要
   - 対象となる Performance Insights メトリクスのフル権限 (*1)
   - 対象となる S3 バケットへのアップロード権限（`"s3:PutObject"` など）

 - Performance Insights や S3 バケットの暗号化にデフォルト以外のキーを使っている場合は、Lambda のロールに対象キーを使用する権限も必要

(*1)

```json:
{
		"Action": "pi:*",
		"Effect": "Allow",
		"Resource": "arn:aws:pi:*:*:metrics/rds/*"
}
```

※`"Resource"`の対象を適切に絞り込む。

## Qiita に書いた紹介記事

 - https://qiita.com/hmatsu47/items/b689db489e75836b0d7d
