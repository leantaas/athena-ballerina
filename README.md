![ballerina](https://i.imgur.com/amjbOf6.png)

# SQL migrations for AWS Athena

## Installation
```
pip install athena-ballerina (COMING SOON)
```

## Usage

Where `migrations_directory` is some directory of form:
```
./migrations
  1_up.sql
  1_down.sql
  2_up.sql
  2_down.sql
  3_up.sql
  3_down.sql
```

Where migration files can be python-formatted strings. In this example `s3_uri` will be replaced with 
`s3://some-bucket/path/to/db` if the parameter is specified with the flag `-p s3_uri s3://some-bucket/path/to/db`:
```hiveql
CREATE EXTERNAL TABLE partitioned_table (
    col_a STRING,
    col_b TIMESTAMP,
    col_c DATE
)
PARTITIONED BY (part_key STRING)
STORED AS PARQUET
LOCATION "{s3_uri}/partitioned_table"
tblproperties ("parquet.compress"="SNAPPY");
```

## CLI

```
usage: ballerina.py [-h] [-m MIGRATION_URI] [-s STAGING_URI] [-w WORK_GROUP]
                    [-d DBNAME] [-D DELIM] [-p PARAM PARAM]
                    [--aws_access_key_id AWS_ACCESS_KEY_ID]
                    [--aws_secret_access_key AWS_SECRET_ACCESS_KEY]
                    [--aws_session_token AWS_SESSION_TOKEN]
                    [--aws_region_name AWS_REGION_NAME]
                    [--aws_profile_name AWS_PROFILE_NAME] [-a AUTO_APPLY_DOWN]
                    [-c AUTO_CLEAN_UP] [-v]
                    migrations_directory

positional arguments:
  migrations_directory  Path to directory containing migrations

optional arguments:
  -h, --help            show this help message and exit
  -m MIGRATION_URI, --migration_uri MIGRATION_URI
                        S3 Migration Dir. (i.e: "s3://my-
                        bucket/path/to/folder/")
  -s STAGING_URI, --staging_uri STAGING_URI
                        Athena Staging dir URI (i.e: "s3://my-
                        bucket/path/to/folder/")
  -w WORK_GROUP, --work_group WORK_GROUP
                        Athena Work Group
  -d DBNAME, --dbname DBNAME
  -D DELIM, --delim DELIM
                        Delimiter used in S3 bucket.
  -p PARAM PARAM, --param PARAM PARAM
                        Parameter that can be formatted into the migration
                        file. For example if "-p KEY VAL" gets passed in CLI,
                        and in the migration file there is a python-formatted
                        string like "LOCATION s3://{KEY}/", it will be
                        formatted to "LOCATION s3://VAL/"
  --aws_access_key_id AWS_ACCESS_KEY_ID
                        AWS Access Key for Boto3
  --aws_secret_access_key AWS_SECRET_ACCESS_KEY
                        AWS Access Secret for Boto3
  --aws_session_token AWS_SESSION_TOKEN
                        AWS Access Session Token for Boto3
  --aws_region_name AWS_REGION_NAME
                        AWS Region Name for Boto3
  --aws_profile_name AWS_PROFILE_NAME
                        AWS Profile Name for Boto3
  -a AUTO_APPLY_DOWN, --auto_apply_down AUTO_APPLY_DOWN
                        Accepts True/False, default is True
  -c AUTO_CLEAN_UP, --auto_clean_up AUTO_CLEAN_UP
                        Should Athena Queries be clean-up from S3
                        OutputLocation? Accepts True/False.
  -v, --version         show program's version number and exit
```

Ballerina is all-or-nothing.

E.g. you are on master branch on revision 5 and want to switch to a feature branch whose latest revision is 4'.
```
1 <- 2 <- 3 <- 4 <- 5  
       \
         3' <- 4' 
```
Applying migrations through Ballerina will leave you on either revision 5 (if an error is encountered) or revision 4' 
(if migration is successful) but not on any of 4, 3, 2, or 3'. 


## Testing on local machine

  * Check `docs/testing.md`

## License

Copyright 2020 LeanTaas, Inc. 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
