```sql
CREATE OR REPLACE STORAGE INTEGRATION my_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::774305612381:role/my-snowflake-role';
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-snowflake-demo-bucket/data/');
  ```

DESC INTEGRATION my_s3_int;	

| Property                        | Type    | Value                                         | Default |
|---------------------------------|---------|-----------------------------------------------|---------|
| ENABLED                          | Boolean | true                                          | false   |
| STORAGE_PROVIDER                 | String  | S3                                            |         |
| STORAGE_ALLOWED_LOCATIONS        | List    | s3://my-snowflake-demo-bucket/data/          | []      |
| STORAGE_BLOCKED_LOCATIONS        | List    |                                               | []      |
| STORAGE_AWS_IAM_USER_ARN         | String  | arn:aws:iam::186297946126:user/18351000-s    |         |
| STORAGE_AWS_ROLE_ARN             | String  | arn:aws:iam::123456789012:role/my-snowflake-role |         |
| STORAGE_AWS_EXTERNAL_ID          | String  | DO41159_SFCRole=3_/LWH27V6VJQMnrAppewESx6f1Qk= |         |
| USE_PRIVATELINK_ENDPOINT         | Boolean | false                                         | false   |
| COMMENT                          | String  |                                               |         |

> this is trusted policy for the role:
> {
>     "Version": "2012-10-17",
>     "Statement": [
>         {
>             "Effect": "Allow",
>             "Principal": {
>                 "AWS": "arn:aws:iam::186297946126:user/18351000-s"
>             },
>             "Action": "sts:AssumeRole",
>             "Condition": {
>                 "StringEquals": {
>                     "sts:ExternalId": "DO41159_SFCRole=3_/LWH27V6VJQMnrAppewESx6f1Qk="
>                 }
>             }
>         }
>     ]
> }

Thanks, now I see. This is the **trust policy** for your AWS IAM role, which controls who can assume the role. Let me break it down clearly:  

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::186297946126:user/18351000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "DO41159_SFCRole=3_/LWH27V6VJQMnrAppewESx6f1Qk="
                }
            }
        }
    ]
}
```

---

### 3️⃣ Check S3 Bucket Permissions

Even if `LIST @my_s3_stage;` fails due to AssumeRole, the role still needs:

- `s3:ListBucket` and `s3:GetObject` permissions on the bucket you configured (`STORAGE_ALLOWED_LOCATIONS`).  

For example, your IAM role policy should include:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:ListBucket",
    "s3:GetObject",
    "s3:PutObject"
  ],
  "Resource": [
    "arn:aws:s3:::my-snowflake-demo-bucket",
    "arn:aws:s3:::my-snowflake-demo-bucket/*"
  ]
}
```

