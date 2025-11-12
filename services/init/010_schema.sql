\set ON_ERROR_STOP on
\timing on

CREATE SCHEMA IF NOT EXISTS registry;
SET search_path TO registry;

-- Удаляем устаревшие таблицы, если остались
DROP TABLE IF EXISTS registry.stage_v8 CASCADE;
DROP TABLE IF EXISTS registry.stage_v27 CASCADE;

-- Утилитарные функции приведения типов
CREATE OR REPLACE FUNCTION registry.parse_date(p text)
RETURNS date LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE d date;
BEGIN
  IF p IS NULL OR btrim(p) = '' THEN RETURN NULL; END IF;
  BEGIN d := to_date(p, 'YYYY-MM-DD'); RETURN d; EXCEPTION WHEN others THEN NULL; END;
  IF d IS NULL THEN BEGIN d := to_date(p, 'DD.MM.YYYY'); EXCEPTION WHEN others THEN d := NULL; END; END IF;
  RETURN d;
END$$;

CREATE OR REPLACE FUNCTION registry.parse_bool(p text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE lower(btrim($1))
           WHEN 'true' THEN true WHEN 't' THEN true WHEN '1' THEN true
           WHEN 'yes' THEN true WHEN 'y' THEN true WHEN 'да' THEN true
           WHEN 'false' THEN false WHEN 'f' THEN false WHEN '0' THEN false
           WHEN 'no' THEN false WHEN 'n' THEN false WHEN 'нет' THEN false
           ELSE NULL END;
$$;

-- Финальная нормализованная таблица
DROP TABLE IF EXISTS registry.reestr CASCADE;
CREATE TABLE registry.reestr (
  id bigserial PRIMARY KEY,
  source_file text,
  Nameoforg text,
  OGRN text,
  INN text,
  Orgaddr text,
  Productmanufaddress text,
  Regnumber text,
  Ektrudp text,
  Docdate date,
  Docvalidtill date,
  Enddate date,
  Registernumber text,
  Productname text,
  OKPD2 text,
  TNVED text,
  Nameofregulations text,
  Score numeric,
  Percentage numeric,
  Scoredesc text,
  Iselectronicproduct boolean,
  Isai boolean,
  ElectronicProductLevel integer,
  Docname text,
  Docdatebasis date,
  Docnum text,
  Docvalidtilltpp date,
  Mptdep text,
  Resdocnum text
);

-- Индексы под типичные фильтры
CREATE INDEX IF NOT EXISTS idx_reestr_regnum ON registry.reestr (Registernumber);
CREATE INDEX IF NOT EXISTS idx_reestr_okpd2 ON registry.reestr (OKPD2);
CREATE INDEX IF NOT EXISTS idx_reestr_tnved ON registry.reestr (TNVED);
CREATE INDEX IF NOT EXISTS idx_reestr_docdate ON registry.reestr (Docdate);
