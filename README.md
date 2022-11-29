# RII
Скрипты для НИИ гриппа

## extract_metadata
Экстрактит метаданные из выгрузки метаданных GISAID (из распакованного .tsv файла или прямо из архива .tar.xz).

Использование:
```bash
extract_metadata <имя файла>
```

Список опций:
```bash
extract_metadata --help
```

Список фильтров:
- Location (фильтрация по подстроке, можно указать несколько для соединения ИЛИ)

Добавляются колонки (флаг `--enrich`):
- ISO (ISO код региона из Virus name)
- RII (флаг загрузки из НИИ)
- Collection month
- Collection week
- Pango lineage cut (Линия Панго, ограниченная двумя секциями цифр)
- Variant cut (Короткое название варианта)

## plot_variant_region_proportion
Рисует диаграмму встречаемости линий и объёма секвенирования по времени и регионам. На вход принимает метаданные из `extract_russian_metadata`.

Список опций:
```bash
./plot_variant_region_proportion.py --help
```
