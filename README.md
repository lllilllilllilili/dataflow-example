# df-guideLine(v.0.0.1)

QuickLab 진행 후에 On-premise 환경에서 GCP와 연동한 ETL 작업

금일 실습한 프로젝트 관련 소스는 [여기](https://github.com/GoogleCloudPlatform/professional-services/tree/main/examples/dataflow-python-examples/batch-examples/cookbook-examples/pipelines) 에서 확인해볼 수 있습니다.

github 관련해서 진행하다 안되시는것이 있으면 -생략- 으로 메일 부탁드립니다. 메일 주실 때는 `소스 코드`, `오류 내용` 등 해결에 도움이 될 수 있는 모든 사진을 부탁드리겠습니다.

## 다운로드 Pycharm or VsCode

`Pycharm`은 통합 개발 환경(IDE)으로 python 개발을 목적으로 설치하게 됩니다.

`vscode`를 사용해도 괜찮습니다.

[Pycharm Download](https://www.jetbrains.com/pycharm/download/) 에서 설치할 수 있습니다.

[Vscode Download](https://code.visualstudio.com/download) 에서 설치할 수 있습니다.

## Git Clone

```
$ git clone https://github.com/cloud-gs-all/df-guideLine.git
```

git clone을 수행하고 프로젝트를 `vscode` 나 `Pycharm` 에서 `import`를 진행합니다.

clone 시 프로젝트 `Directory` 구조는 아래와 같습니다.

`public` : README에 포함될 `.gif` 파일 모음

`schema` : dataflow 실행 시 `bigQuery` 데이터를 인입하기 위해 참고하는 스키마 정보

`LICENSE` : apache-beam open source에 대한 라이센스

`df-v0.1.py` : dataflow 소스 코드, python으로 작성되어 있습니다.

`README.md` : 현재 보고 있는 파일

`requirements.txt` : `df-v0.1.py`를 동작시키기 위한 python의 패키지 목록

```
|-- project
|  `-- public
|  `-- schema
|     `-- mv_sa_tran_online.json
|  `-- LICENSE
|  `-- df-v0.1.py
|  `-- README.md
|  `-- requirements.txt
|--
```

## Google Cloud SDK

사전에 `Google Cloud SDK` 가 설치 되어있어야 합니다.

`gcloud init` 으로 `google cloud platform` 과 연결합니다.

```
$ gcloud init
```

[Google Cloud SDK Docs](https://cloud.google.com/sdk/docs/install?hl=ko) 참고 부탁드립니다.

## IAM 설정

IAM role에 이용하고 계신 email에 Role이 `Computer Admin` 권한 이상이라면 `dataflow` 동작 시키는데 어려움이 없습니다.

정석적인 방법으로는 `Service Account` 를 만들고 `Dataflow Worker` 권한을 준 뒤에 `JSON` 파일로 다운 받고 환경설정을 셋팅 하시면 됩니다.

MAC 의 경우에 아래와 같이 작성합니다

```
$ vi ~./zshrc 혹은 vi ~/.bashrc
```

```
export GOOGLE_APPLICATION_CREDENTIALS=[Service Account File Path]
```

[IAM 설정 Docs](https://cloud.google.com/docs/authentication/getting-started) 참고 부탁드립니다.

## df-v0.1.py 실행

python 가상 환경으로 이동하고 실행하는데 필요한 패키지를 설치합니다.

```
$ cd dataflow-example
$ sudo pip install virtualenv
$ virtualenv -p python3 venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

`df-v0.1.py` 소스코드를 살펴보면 `add_argument` 로 등록된 `project`, `bucket`, `dataSet` 을 python 파일을 실행시켜 줄 때 `parameter` 로 넘겨주어야 합니다.

```
$ python3 df-v0.1.py -p [project-name] -b [bucket-name] -d [bigQuery-dataSet]
```

실행이 완료되면 `Google Cloud Platform`으로 이동해서 `dataflow` 에서 동작하고 있는지 확인합니다.

## df-v0.1 소스코드

### Schema

현재 위치에서 `schema` 파일 경로를 찾아 bigQuery로 인입 시 schema로 등록하여 사용하게 됩니다.

```python
class DataTransformation:
    def __init__(self):
        dir_path = os.path.abspath(os.path.dirname(__file__))
        self.schema_str = ""
        schema_file = os.path.join(dir_path, "schema", "mv_sa_tran_online.json")
        with open(schema_file) \
                as f:
            data = f.read()
            #self.schema_str = data
            self.schema_str = '{"fields": ' + data + '}'

    data_ingestion = DataTransformation()
    schema = parse_table_schema_from_json(data_ingestion.schema_str)

    additional_bq_parameters = {
        "allowJaggedRows": True,
        "allowQuotedNewlines": True,
    }

    (Input
     | 'totablerow' >> beam.Map(lambda fields: create_row(fields))
     | 'out' >> beam.io.WriteToBigQuery(
                events_output, schema=schema,
                additional_bq_parameters=additional_bq_parameters,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
     )

```

### Transform

`csv` 파일을 읽어서 `bigQuery` 테이블에 인입하기 위해 `json` 형태로 데이터를 변형시켜줍니다.

```python
def create_row(fields):
    header = 'sale_date,store_code,register_id,voucher_id,voucher_seq,product_code,sale_qty,sale_amount,sale_vat_amount,profit_amount,customer_code,mall_product_code,mall_flag,order_channel,mall_com_id,deli_type,order_id,mall_order_flag,mall_deli_flag,mall_order_cnt_flag,mall_deli_cnt_flag,rece_address,rece_detail_address,customer_cnt,order_cnt,post_id,deli_order,order_deli_date,sale_flag,org_order_code,picker_id,sale_hour,sold_stock_flag,card_name,payment_flag,vendor_code,purchase_type,sale_channel_flag,basic_amount,sold_stock_qty,order_code,real_order_flag,order_name,class_code,order_wanted_deli_date,region_id,picking_sold_flag,deli_id,coupon_flag,picking_sta_date,picking_end_date,order_deli_sta_date,order_deli_end_date,picking_time,order_time,reserve_date,pro_option_value,order_deli_method,dong_code,offline_order_flag,add_order_org_order_code,deli_start_date,original_code,reserve_deli_order,voucher_code1,voucher_code2,group_region_id,member_code,sold_amount,replace_amount'.split(
        ',')


    featdict = {}


    for name, value in zip(header, fields):
        featdict[name] = value


    return featdict
```

### PipeLine 구성

PipeLine 구성에 아래 코드를 보시면 `beam.io.ReadFromText` 는 GCS(Google Cloud Storage) 에서 데이터를 읽어오는 것을 시작으로

읽어온 데이터를 `beam.Map(lambda line: next(csv.reader([line])))` 을 이용해서 line by line로 데이터로 Transform을 수행하게 됩니다.

마지막에는 `beam.io.WriteToBigQuery` 으로 `bigQuery` 에 읽어온 데이터를 적재하게 됩니다.

중간에 보이는 `additional_bq_parameters`는 `mv_sa_tran_online` 데이터 적재 시 발생한 이슈에 따라 사용한 옵션 입니다.

`beam.io.gcp.WriteToBigQuery()` 함수가 기본적으로 BigQuery api를 랩핑해서 만든 것이기 때문에 load job시 옵션들을 주었습니다.

`allowQuotedNewlines` : CSV파일에서 따옴표 안에 줄바꿈 문자가 포함된 데이터 섹션을 허용할지 여부를 표기합니다.

`allowJaggedRows` : 뒤에 오는 열이 누락된 행을 허용합니다. 누란된 값은 null로 취급합니다.

```python
Input = (pipeline
                | 'read' >> beam.io.ReadFromText(filename, skip_header_lines=1)
                | 'fields' >> beam.Map(lambda line: next(csv.reader([line])))

                )

    data_ingestion = DataTransformation()
    schema = parse_table_schema_from_json(data_ingestion.schema_str)
    additional_bq_parameters = {
        "allowJaggedRows": True,
        "allowQuotedNewlines": True,
    }

    (Input
     | 'totablerow' >> beam.Map(lambda fields: create_row(fields))
     | 'out' >> beam.io.WriteToBigQuery(
                events_output, schema=schema,
                additional_bq_parameters=additional_bq_parameters,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
     )

```

### Dataflow 실행 옵션

소스 코드를 말아서 올릴 때 Dataflow 실행 시 `project명`, `job_names`, `save_main_session`, `staging_location`, `temp_location`, `runner`를 작성해 주어야 합니다.

그 외 실행 옵션은 하기 문서에서 자세히 살펴볼 수 있습니다.

[Cloud Dataflow 파이프라인 옵션 설정](https://www.google.com/search?q=dataflow+%EC%8B%A4%ED%96%89+%EB%8F%99%EC%9E%91+option&rlz=1C5CHFA_enKR907KR907&oq=dataflow+%EC%8B%A4%ED%96%89+%EB%8F%99%EC%9E%91+option&aqs=chrome..69i57j33i160.6237j0j4&sourceid=chrome&ie=UTF-8)

```python
argv = [
        '--project={0}'.format(project),
        '--job_name=dataflow-job',
        '--save_main_session',
        '--staging_location=gs://df-workspace/df_staging/',
        '--temp_location=gs://df-workspace/df_temp/',
        #'--template_location=gs://alex_dataflow_template/MyTemplate',
        '--runner=Dataflow' #Dataflow or DirectRunner
    ]
```

## Dataflow 소스코드 작성

`dataflow`를 실행시키기 위해서는 `apache-beam`을 기반으로 하고 있기 때문에 `apache-beam` 공식 문서를 보며 개발을 진행하셔야 합니다.

세 가지 언어 `python`, `java`, `Go` 를 지원합니다.

소스 코드가 간결하고 이해하기 쉬운 `python`으로 작성하는것을 추천 드립니다.

그 외 자세한 내용은 하기 문서에서 자세히 살펴볼 수 있습니다.

[Apache Beam Docs](https://beam.apache.org/documentation/runners/dataflow/)

[Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)

[Apache Beam SDK Docs for Python](https://beam.apache.org/releases/pydoc/2.13.0/index.html)
