-- google_adapt.sql — DuckDB macros for Google Drive / Sheets / Docs APIs
--
-- Requires: blobhttp extension, blobtemplates extension, postgres scanner
-- Must be sourced AFTER attaching PG and loading extensions:
--
--   ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES);
--   LOAD 'bhttp';
--   LOAD 'blobtemplates';
--   .read sql/google_adapt.sql
--
-- All macros take access_token and quota_project as parameters.
-- The caller is responsible for providing a fresh token (from OpenBao,
-- gcloud auth print-access-token, or bh_http_config_set_bearer).
--
-- Adapter definitions live in google.yaml → domain.http_adapter.
-- These macros can also work against a DuckDB-local http_adapter table
-- populated via load_http_adapters.sql — just remove the pg. prefix.

-- ═══════════════════════════════════════════════════════════════════
-- Helper: Google auth headers MAP
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_auth_headers(access_token, quota_project) AS
    MAP {
        'Authorization':      'Bearer ' || access_token,
        'X-Goog-User-Project': quota_project
    };

-- ═══════════════════════════════════════════════════════════════════
-- google_drive_list(folder_id, access_token, quota_project)
--
-- List files in a Google Drive folder.
-- Returns: id, name, mime_type, modified, size
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_drive_list(folder_id, access_token, quota_project) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('folderId', folder_id)),
            headers := google_auth_headers(access_token, quota_project)
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'google_drive_list_folder'
    ),
    FILES AS (
        SELECT unnest(from_json(doc->'files', '["json"]')) AS f
        FROM RAW
    )
    SELECT f->>'id'           AS id,
           f->>'name'         AS name,
           f->>'mimeType'     AS mime_type,
           f->>'modifiedTime' AS modified,
           f->>'size'         AS size
    FROM FILES
);

-- ═══════════════════════════════════════════════════════════════════
-- google_drive_metadata(file_id, access_token, quota_project)
--
-- Get metadata for a single Drive file.
-- Returns: id, name, mime_type, size, modified, parents
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_drive_metadata(file_id, access_token, quota_project) AS TABLE (
    SELECT doc->>'id'           AS id,
           doc->>'name'         AS name,
           doc->>'mimeType'     AS mime_type,
           doc->>'size'         AS size,
           doc->>'modifiedTime' AS modified,
           doc->'parents'       AS parents_json
    FROM (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('fileId', file_id)),
            headers := google_auth_headers(access_token, quota_project)
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'google_drive_file_metadata'
    )
);

-- ═══════════════════════════════════════════════════════════════════
-- google_sheet_tabs(spreadsheet_id, access_token, quota_project)
--
-- List sheet tabs in a Google spreadsheet.
-- Returns: title, sheet_id, row_count, column_count
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_sheet_tabs(spreadsheet_id, access_token, quota_project) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('spreadsheetId', spreadsheet_id)),
            headers := google_auth_headers(access_token, quota_project)
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'google_sheet_tabs'
    ),
    TABS AS (
        SELECT unnest(from_json(doc->'sheets', '["json"]')) AS t
        FROM RAW
    )
    SELECT t->'properties'->>'title'                                  AS title,
           CAST(t->'properties'->>'sheetId' AS INTEGER)               AS sheet_id,
           CAST(t->'properties'->'gridProperties'->>'rowCount' AS INTEGER)    AS row_count,
           CAST(t->'properties'->'gridProperties'->>'columnCount' AS INTEGER) AS column_count
    FROM TABS
);

-- ═══════════════════════════════════════════════════════════════════
-- google_sheet_values(spreadsheet_id, range, access_token, quota_project)
--
-- Read cell values from a sheet range (A1 notation).
-- Returns: row_num, values_json (array of cell values per row)
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_sheet_values(spreadsheet_id, range, access_token, quota_project) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('spreadsheetId', spreadsheet_id, 'range', range)),
            headers := google_auth_headers(access_token, quota_project)
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'google_sheet_values'
    ),
    ROWS AS (
        SELECT unnest(from_json(doc->'values', '["json"]')) AS row_data,
               generate_subscripts(from_json(doc->'values', '["json"]'), 1) AS row_num
        FROM RAW
    )
    SELECT row_num,
           row_data AS values_json
    FROM ROWS
);

-- ═══════════════════════════════════════════════════════════════════
-- Examples
-- ═══════════════════════════════════════════════════════════════════

-- Set up token (get from OpenBao, gcloud, or Python):
--   SET VARIABLE google_token = 'ya29.a0...';
--   SET VARIABLE google_project = 'meplex-integration';
--
-- List files in a shared folder:
--   SELECT * FROM google_drive_list(
--       '1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT',
--       getvariable('google_token'),
--       getvariable('google_project')
--   );
--
-- Get metadata for a specific file:
--   SELECT * FROM google_drive_metadata(
--       '1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT',
--       getvariable('google_token'),
--       getvariable('google_project')
--   );
--
-- List tabs in a spreadsheet:
--   SELECT * FROM google_sheet_tabs(
--       '13uaR6lb314XJ7ytIcFAqcjXfeVezstQIpDHBlN9fMuA',
--       getvariable('google_token'),
--       getvariable('google_project')
--   );
--
-- Read cell values:
--   SELECT * FROM google_sheet_values(
--       '13uaR6lb314XJ7ytIcFAqcjXfeVezstQIpDHBlN9fMuA',
--       'Sheet1!A1:Z50',
--       getvariable('google_token'),
--       getvariable('google_project')
--   );
--
-- Join: list folder then get metadata for each file:
--   SELECT f.name, m.*
--   FROM google_drive_list('1C01...', getvariable('google_token'), getvariable('google_project')) AS f,
--        LATERAL google_drive_metadata(f.id, getvariable('google_token'), getvariable('google_project')) AS m;
