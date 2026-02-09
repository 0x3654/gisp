#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

start_time=$(date +%s)
SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EMBED_SCRIPT="${SCRIPT_DIR}/update_embeddings.py"
AUTO_EMBED=${AUTO_EMBED:-1}

FILES_DIR=${FILES_DIR}
DB=registry

hash_file() {
  local f="$1"
  sha256sum "$f" | awk '{print $1}'
}

detect_header() {
  local f="$1"
  python3 - "$f" <<'PY'
import csv, sys
with open(sys.argv[1], newline='', encoding='utf-8') as fh:
    r = csv.reader(fh)
    hdr = next(r)
    print(','.join(hdr))
PY
}

preprocess_csv_dynamic() {
  local in="$1"
  local out="$2"
  python3 - "$in" "$out" <<'PY'
import csv
import sys

infile = sys.argv[1]
outfile = sys.argv[2]

with open(infile, newline='', encoding='utf-8') as inf, open(outfile, 'w', newline='', encoding='utf-8') as outf:
    reader = csv.DictReader(inf)
    fields = reader.fieldnames
    writer = csv.DictWriter(outf, fieldnames=fields)
    writer.writeheader()
    seen = set()
    for row in reader:
        tnved_value = row.get('TNVED', '') if 'TNVED' in fields else ''
        codes = [code.strip() for code in tnved_value.split(';')] if tnved_value else ['']
        codes = [''.join(ch for ch in code if ch.isdigit()) for code in codes]
        for code in codes:
            new_row = row.copy()
            if 'TNVED' in fields:
                new_row['TNVED'] = code
            row_tuple = tuple(new_row[col] for col in fields)
            if row_tuple not in seen:
                writer.writerow(new_row)
                seen.add(row_tuple)
PY
}

psql_exec() {
  PGPASSWORD="${POSTGRES_PASSWORD}" psql \
    -h "${POSTGRES_HOST}" \
    -p "${POSTGRES_PORT}" \
    -U "${POSTGRES_USER}" \
    -d "$DB" \
    -v ON_ERROR_STOP=1 \
    --tuples-only --no-align "$@"
}

psql_exec_quiet() {
  psql_exec "$@" >/dev/null
}

mapfile -t csv_files < <(ls -1t "$FILES_DIR"/data-*.csv 2>/dev/null || true)

if [[ ${#csv_files[@]} -eq 0 ]]; then
  echo "⚠️  CSV-файлы в каталоге $FILES_DIR не найдены"
  exit 0
fi

latest_file="${csv_files[0]}"
name=$(basename "$latest_file")
file_date=$(echo "$name" | grep -oE '[0-9]{8}' | head -1 || true)
file_date_fmt="-"
if [[ -n "$file_date" && ${#file_date} -eq 8 ]]; then
  file_date_fmt=$(date -d "${file_date:0:4}-${file_date:4:2}-${file_date:6:2}" +%d.%m.%Y 2>/dev/null || echo "-")
fi

echo ""
echo "($file_date_fmt) 📦 Последний файл: $name"
header_count=$(detect_header "$latest_file" | awk -F',' '{print NF}')
rows_total=$(wc -l < "$latest_file")
rows_no_header=$((rows_total > 0 ? rows_total - 1 : 0))

size=$(stat -c%s "$latest_file")
sha=$(hash_file "$latest_file")

already_imported=$(psql_exec <<EOF
SELECT 1
FROM registry.load_log
WHERE file_name = '$name'
  AND file_sha256 = '$sha'
LIMIT 1;
EOF
)
already_imported=$(echo "$already_imported" | tr -d '[:space:]')
if [[ "$already_imported" == "1" ]]; then
  echo "✅ Файл уже загружен в базу (load_log). Пропускаем импорт."
  exit 0
fi

tmpfile=$(mktemp "$FILES_DIR/tmp.$name.XXXXXX")
trap 'rm -f "$tmpfile"' EXIT
preprocess_csv_dynamic "$latest_file" "$tmpfile"

if [[ ! -s "$tmpfile" ]]; then
  echo "❌ Предобработанный файл пуст"
  exit 1
fi

psql_exec_quiet -c "
SET client_min_messages TO warning;
DROP TABLE IF EXISTS registry.tmp_latest_raw;
"
psql_exec_quiet -c "CREATE TABLE registry.tmp_latest_raw (
  Nameoforg text,
  OGRN text,
  INN text,
  Orgaddr text,
  Productmanufaddress text,
  Regnumber text,
  Ektrudp text,
  Docdate text,
  Docvalidtill text,
  Enddate text,
  Registernumber text,
  Productname text,
  OKPD2 text,
  TNVED text,
  Nameofregulations text,
  Score text,
  Percentage text,
  Scoredesc text,
  Iselectronicproduct text,
  Isai text,
  ElectronicProductLevel text,
  Docname text,
  Docdatebasis text,
  Docnum text,
  Docvalidtilltpp text,
  Mptdep text,
  Resdocnum text
)"

psql_exec_quiet <<EOF
\copy registry.tmp_latest_raw FROM '$(realpath "$tmpfile")' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"', ENCODING 'UTF8')
EOF

psql_exec_quiet -c "
SET client_min_messages TO warning;
DROP TABLE IF EXISTS registry.tmp_latest_norm;
"
psql_exec_quiet -c "
CREATE TABLE registry.tmp_latest_norm AS
SELECT DISTINCT
  '$name'::text AS source_file,
  NULLIF(NULLIF(btrim(Nameoforg), '-'), '') AS Nameoforg,
  NULLIF(NULLIF(btrim(OGRN), '-'), '') AS OGRN,
  NULLIF(NULLIF(btrim(INN), '-'), '') AS INN,
  NULLIF(NULLIF(btrim(Orgaddr), '-'), '') AS Orgaddr,
  NULLIF(NULLIF(btrim(Productmanufaddress), '-'), '') AS Productmanufaddress,
  NULLIF(NULLIF(btrim(Regnumber), '-'), '') AS Regnumber,
  NULLIF(NULLIF(btrim(Ektrudp), '-'), '') AS Ektrudp,
  registry.parse_date(Docdate) AS Docdate,
  registry.parse_date(Docvalidtill) AS Docvalidtill,
  registry.parse_date(Enddate) AS Enddate,
  NULLIF(NULLIF(btrim(Registernumber), '-'), '') AS Registernumber,
  NULLIF(NULLIF(btrim(Productname), '-'), '') AS Productname,
  NULLIF(NULLIF(btrim(OKPD2), '-'), '') AS OKPD2,
  NULLIF(NULLIF(btrim(TNVED), '-'), '') AS TNVED,
  NULLIF(NULLIF(btrim(Nameofregulations), '-'), '') AS Nameofregulations,
  CASE WHEN trim(Score) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN trim(Score)::numeric END AS Score,
  CASE WHEN trim(Percentage) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN trim(Percentage)::numeric END AS Percentage,
  NULLIF(NULLIF(btrim(Scoredesc), '-'), '') AS Scoredesc,
  CASE
    WHEN lower(trim(Iselectronicproduct)) IN ('да','true','t','1','yes','y') THEN true
    WHEN lower(trim(Iselectronicproduct)) IN ('нет','false','f','0','no','n') THEN false
    ELSE NULL
  END AS Iselectronicproduct,
  CASE
    WHEN lower(trim(Isai)) IN ('да','true','t','1','yes','y') THEN true
    WHEN lower(trim(Isai)) IN ('нет','false','f','0','no','n') THEN false
    ELSE NULL
  END AS Isai,
  CASE
    WHEN regexp_replace(coalesce(ElectronicProductLevel, ''), '\\D', '', 'g') <> ''
      THEN regexp_replace(ElectronicProductLevel, '\\D', '', 'g')::integer
    ELSE NULL
  END AS ElectronicProductLevel,
  NULLIF(NULLIF(btrim(Docname), '-'), '') AS Docname,
  registry.parse_date(Docdatebasis) AS Docdatebasis,
  NULLIF(NULLIF(btrim(Docnum), '-'), '') AS Docnum,
  registry.parse_date(Docvalidtilltpp) AS Docvalidtilltpp,
  NULLIF(NULLIF(btrim(Mptdep), '-'), '') AS Mptdep,
  NULLIF(NULLIF(btrim(Resdocnum), '-'), '') AS Resdocnum
FROM registry.tmp_latest_raw
"

rows_norm_raw=$(psql_exec -c "SELECT count(*) FROM registry.tmp_latest_norm" | tr -d '[:space:]')
rows_norm=${rows_norm_raw:-0}
rows_norm=$((rows_norm + 0))
printf "Столбцов в заголовке: %d\n" "$header_count"
printf "Строк в исходном файле:%12d\n" "$rows_no_header"
printf "Строк после нормализации: %d\n" "$rows_norm"

diff_output=$(psql_exec <<'EOF'
WITH new_data AS (
  SELECT *,
         md5(concat_ws('||',
                       COALESCE(Nameoforg,''), COALESCE(OGRN,''), COALESCE(INN,''),
                       COALESCE(Orgaddr,''), COALESCE(Productmanufaddress,''),
                       COALESCE(Regnumber,''), COALESCE(Ektrudp,''),
                       COALESCE(Docdate::text,''), COALESCE(Docvalidtill::text,''),
                       COALESCE(Enddate::text,''), COALESCE(Registernumber,''),
                       COALESCE(Productname,''), COALESCE(OKPD2,''), COALESCE(TNVED,''),
                       COALESCE(Nameofregulations,''), COALESCE(Score::text,''),
                       COALESCE(Percentage::text,''), COALESCE(Scoredesc,''),
                       COALESCE(Iselectronicproduct::text,''), COALESCE(Isai::text,''),
                       COALESCE(ElectronicProductLevel::text,''), COALESCE(Docname,''),
                       COALESCE(Docdatebasis::text,''), COALESCE(Docnum,''),
                       COALESCE(Docvalidtilltpp::text,''), COALESCE(Mptdep,''),
                       COALESCE(Resdocnum,''))) AS row_hash
  FROM registry.tmp_latest_norm
),
existing AS (
  SELECT id,
         md5(concat_ws('||',
                       COALESCE(Nameoforg,''), COALESCE(OGRN,''), COALESCE(INN,''),
                       COALESCE(Orgaddr,''), COALESCE(Productmanufaddress,''),
                       COALESCE(Regnumber,''), COALESCE(Ektrudp,''),
                       COALESCE(Docdate::text,''), COALESCE(Docvalidtill::text,''),
                       COALESCE(Enddate::text,''), COALESCE(Registernumber,''),
                       COALESCE(Productname,''), COALESCE(OKPD2,''), COALESCE(TNVED,''),
                       COALESCE(Nameofregulations,''), COALESCE(Score::text,''),
                       COALESCE(Percentage::text,''), COALESCE(Scoredesc,''),
                       COALESCE(Iselectronicproduct::text,''), COALESCE(Isai::text,''),
                       COALESCE(ElectronicProductLevel::text,''), COALESCE(Docname,''),
                       COALESCE(Docdatebasis::text,''), COALESCE(Docnum,''),
                       COALESCE(Docvalidtilltpp::text,''), COALESCE(Mptdep,''),
                       COALESCE(Resdocnum,''))) AS row_hash
  FROM registry.reestr
),
to_delete AS (
  SELECT e.id
  FROM existing e
  LEFT JOIN new_data n USING (row_hash)
  WHERE n.row_hash IS NULL
),
deleted AS (
  DELETE FROM registry.reestr r
  USING to_delete d
  WHERE r.id = d.id
  RETURNING r.id
),
to_insert AS (
  SELECT n.*
  FROM new_data n
  LEFT JOIN existing e USING (row_hash)
  WHERE e.row_hash IS NULL
),
inserted AS (
  INSERT INTO registry.reestr (
    source_file, Nameoforg, OGRN, INN, Orgaddr, Productmanufaddress,
    Regnumber, Ektrudp, Docdate, Docvalidtill, Enddate, Registernumber,
    Productname, OKPD2, TNVED, Nameofregulations, Score, Percentage,
    Scoredesc, Iselectronicproduct, Isai, ElectronicProductLevel, Docname,
    Docdatebasis, Docnum, Docvalidtilltpp, Mptdep, Resdocnum
  )
  SELECT
    source_file, Nameoforg, OGRN, INN, Orgaddr, Productmanufaddress,
    Regnumber, Ektrudp, Docdate, Docvalidtill, Enddate, Registernumber,
    Productname, OKPD2, TNVED, Nameofregulations, Score, Percentage,
    Scoredesc, Iselectronicproduct, Isai, ElectronicProductLevel, Docname,
    Docdatebasis, Docnum, Docvalidtilltpp, Mptdep, Resdocnum
  FROM to_insert
  RETURNING id
)
SELECT
  COALESCE((SELECT count(*) FROM deleted), 0),
  COALESCE((SELECT count(*) FROM to_insert), 0),
  COALESCE((SELECT count(*) FROM inserted), 0);
EOF
)

deleted_count=$(echo "$diff_output" | awk -F'|' 'NR==1{print $1+0}')
missing_count=$(echo "$diff_output" | awk -F'|' 'NR==1{print $2+0}')
inserted_count=$(echo "$diff_output" | awk -F'|' 'NR==1{print $3+0}')

printf "🔄 Синхронизация: удалено %'d | новых %'d | вставлено %'d\n" \
  "$deleted_count" "$missing_count" "$inserted_count"

psql_exec_quiet -c "INSERT INTO registry.load_log(file_name,file_size,file_sha256,rows_inserted)
              VALUES ('$name',$size,'$sha',$inserted_count)
              ON CONFLICT DO NOTHING;"

if [[ "$AUTO_EMBED" == "1" ]]; then
  if [[ -f "$EMBED_SCRIPT" ]]; then
    if (( inserted_count > 0 )); then
      echo "🧠 Обновляем эмбеддинги для файла $name..."
      if python3 "$EMBED_SCRIPT" --source-file "$name"; then
        echo "🧠 Эмбеддинги обновлены"
      else
        echo "❌ Ошибка при обновлении эмбеддингов"
        exit 1
      fi
    else
      echo "🧠 Эмбеддинги: новых строк нет, обновление пропущено"
    fi
  else
    echo "⚠️  Скрипт обновления эмбеддингов не найден ($EMBED_SCRIPT). Пропускаем шаг."
  fi
else
  echo "🧠 Эмбеддинги: автообновление отключено (AUTO_EMBED=$AUTO_EMBED)"
fi

# Clean up staging tables for the next run
psql_exec_quiet -c "
SET client_min_messages TO warning;
DROP TABLE IF EXISTS registry.tmp_latest_norm;
"
psql_exec_quiet -c "
SET client_min_messages TO warning;
DROP TABLE IF EXISTS registry.tmp_latest_raw;
"

end_time=$(date +%s)
elapsed=$((end_time - start_time))
printf "⏱ Прошедшее время: %02d:%02d:%02d\n" $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60))
echo "✅ Импорт завершён: $(date '+%d.%m.%Y %H:%M:%S')"
