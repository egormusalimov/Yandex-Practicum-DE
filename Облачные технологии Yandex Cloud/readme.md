# Облачные технологии Yandex Cloud

## Описание
Строим сервис, который заполняет данными детальный слой DWH в Postgres. Сбор данных осуществляется из Redis+Kafka. Используем технологии Kafka и Kubernetes.

## Цель работы
Реализовать два сервиса, которые заполняют слои DDS и CDM хранилища в PostgresSQL. При реализации использовать Yandex Cloud.

## Этапы работы
- Создание сервиса заполнения слоя DDS.  
  1.1 Написание кода сервиса.  
  1.2 Реализация сервиса в Kubernetes.
- Создание сервиса заполнения слоя CDM.  
  2.1 Написание кода сервиса.  
  2.2 Реализация сервиса в Kubernetes.

## Описание рабочих файлов

### Папка `solution/service_dds/src`

- **app_config.py** — параметры подключения к Kafka.

### Папка `solution/service_dds/src/dds_loader`

- **dds_message_processor_job.py** — загрузчик dds слоя.

### Папка `solution/service_dds/src/dds_loader/repository`

- **dds_repository.py** — код заполнения слоя dds.

### Папка `solution/service_cdm/src`

- **app_config.py** — параметры подключения к Kafka.

### Папка `solution/service_cdm/src/cdm_loader`

- **cdm_message_processor_job.py** — загрузчик cdm слоя.

### Папка `solution/service_cdm/src/cdm_loader/repository`

- **cdm_repository.py** — код заполнения слоя витрин.

