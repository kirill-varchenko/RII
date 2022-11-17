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
