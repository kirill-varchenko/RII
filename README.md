# RII
Скрипты для НИИ гриппа

## extract_russian_metadata
Экстрактит российские метаданные из выгрузки метаданных GISAID (из распакованного .tsv файла или прямо из архива .tar.xz).

Использование:
```bash
./extract_russian_metadata.py <имя файла>
```

Выход: `russian-metadata-YYYY-MM-DD.tsv`

Добавляются колонки:
- ISO (ISO код региона из Virus name)
- RII (флаг загрузки из НИИ)
- Collection month
- Collection week
- Pango lineage cut (Линия Панго, ограниченная двумя секциями цифр)
- Variant cut (Короткое название варианта)

## vgarus
Загружает сиквенсы и метаданные в систему VGARUS. Для отправки нужен json файл с данными, который можно сформировать из метаданных в формате tsv и fasta файла с помощью команды `combine-package`. Креды для подключения можно передать либо через опции `--username`, `--password`, либо в переменных окружения `VGARUS_USERNAME`, `VGARUS_PASSWORD`, либо через env файл. 

Список команд и опций:
```bash
vgarus --help
```

## plot_variant_region_proportion
Рисует диаграмму встречаемости линий и объёма секвенирования по времени и регионам. На вход принимает метаданные из `extract_russian_metadata`.

Список опций:
```bash
./plot_variant_region_proportion.py --help
```
