# Проект для практикческих заданий по DVC
## Требований к окружению
- Python  3.7/3.8
- Установите необходимые пакеты `pip install -r requirements.txt`

## Параметры для DVC
Для хранения данных DVC будет использовать S3 (Yandex.Cloud).
Необходимо выполнить настройку AWS на вашем рабочем ПК.

`config` - AWS Config
```
[default]
region=ru-central1
output=json
```
`credentials` - AWS credentials
```
[default]
aws_access_key_id=#######
aws_secret_access_key=############
s3 =
  endpoint_url=https://storage.yandexcloud.net
```
Ключи предоставлены в первом шаге раздела - Практика, Урок №2 "Версионированиие данных (DVC)"