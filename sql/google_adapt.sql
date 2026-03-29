-- google_adapt.sql — DuckDB macros for Google Drive / Sheets / Docs APIs
--
-- Requires: blobhttp extension, blobtemplates extension
-- Adapter definitions from google.yaml must be in http_adapter table
-- (either pg.domain.http_adapter or DuckDB-local via load_http_adapters.sql).
--
-- Setup:
--   .read sql/google_adapt.sql
--   SET VARIABLE bh_http_config = google_init();
--   SELECT * FROM google_drive_list('1C01bJD...');
--
-- google_init() configures blobhttp to fetch the bearer token from
-- OpenBao at request time. The token must be pre-populated in vault
-- at secret/blobapi/google_token (use tools/google_token_refresh.py).
--
-- For explicit-token use without vault, set the bearer token directly:
--   SET VARIABLE bh_http_config = bh_http_config_set_bearer(
--       'https://www.googleapis.com/', 'ya29.a0...');
--   SET VARIABLE bh_http_config = bh_http_config_set_bearer(
--       'https://sheets.googleapis.com/', 'ya29.a0...');
--   SET VARIABLE google_project = 'meplex-integration';
-- Then the same macros work — blobhttp uses the bearer token from config.

-- ═══════════════════════════════════════════════════════════════════
-- google_init — configure blobhttp vault integration for Google APIs
--
-- This is a helper scalar macro, not a table macro, because it must
-- be used with SET VARIABLE to persist the config:
--
--   SET VARIABLE bh_http_config = google_init();
--
-- After this, blobhttp fetches the bearer token from OpenBao at
-- secret/blobapi/google_token for all requests to googleapis.com.
--
-- Optionally set these variables before calling:
--   SET VARIABLE google_vault_addr  = 'http://127.0.0.1:8200';
--   SET VARIABLE google_vault_token = 'dev-blobapi-token';
--   SET VARIABLE google_project     = 'meplex-integration';
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_vault_config() AS
    json_object(
        'auth_type',    'bearer',
        'vault_addr',   COALESCE(TRY_CAST(getvariable('google_vault_addr') AS VARCHAR),  'http://127.0.0.1:8200'),
        'vault_token',  COALESCE(TRY_CAST(getvariable('google_vault_token') AS VARCHAR), 'dev-blobapi-token'),
        'vault_path',   'secret/blobapi/google_token',
        'vault_field',  'access_token'
    );

CREATE OR REPLACE MACRO google_init() AS
    map_concat(
        _bh_http_config(),
        MAP {
            'https://www.googleapis.com/':    CAST(google_vault_config() AS VARCHAR),
            'https://sheets.googleapis.com/': CAST(google_vault_config() AS VARCHAR)
        }
    );

-- ═══════════════════════════════════════════════════════════════════
-- Helper: quota project header
--
-- blobhttp injects Authorization from vault automatically.
-- We only need to add X-Goog-User-Project for quota billing.
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_project_header() AS
    MAP {
        'X-Goog-User-Project': COALESCE(
            TRY_CAST(getvariable('google_project') AS VARCHAR),
            'meplex-integration'
        )
    };

-- ═══════════════════════════════════════════════════════════════════
-- google_drive_list(folder_id)
--
-- List files in a Google Drive folder.
-- Returns: id, name, mime_type, modified, size
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_drive_list(folder_id) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('folderId', folder_id)),
            headers := google_project_header()
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
-- google_drive_metadata(file_id)
--
-- Get metadata for a single Drive file.
-- Returns: id, name, mime_type, size, modified, parents
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_drive_metadata(file_id) AS TABLE (
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
            headers := google_project_header()
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'google_drive_file_metadata'
    )
);

-- ═══════════════════════════════════════════════════════════════════
-- google_sheet_tabs(spreadsheet_id)
--
-- List sheet tabs in a Google spreadsheet.
-- Returns: title, sheet_id, row_count, column_count
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_sheet_tabs(spreadsheet_id) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('spreadsheetId', spreadsheet_id)),
            headers := google_project_header()
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
-- google_sheet_values(spreadsheet_id, range)
--
-- Read cell values from a sheet range (A1 notation).
-- Returns: row_num, values_json (array of cell values per row)
-- ═══════════════════════════════════════════════════════════════════

CREATE OR REPLACE MACRO google_sheet_values(spreadsheet_id, range) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            bt_template_render(a.url_template,
                json_object('spreadsheetId', spreadsheet_id, 'range', range)),
            headers := google_project_header()
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

-- ── Setup ────────────────────────────────────────────────────────
--   .read sql/google_adapt.sql
--   SET VARIABLE bh_http_config = google_init();
--
-- ── List files ───────────────────────────────────────────────────
--   SELECT * FROM google_drive_list('1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT');
--
-- ── Metadata ─────────────────────────────────────────────────────
--   SELECT * FROM google_drive_metadata('1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT');
--
-- ── Sheet tabs ───────────────────────────────────────────────────
--   SELECT * FROM google_sheet_tabs('13uaR6lb314XJ7ytIcFAqcjXfeVezstQIpDHBlN9fMuA');
--
-- ── Cell values ──────────────────────────────────────────────────
--   SELECT * FROM google_sheet_values('13uaR6lb...', 'Sheet1!A1:Z50');
--
-- ── Compose: folder → spreadsheet tabs ───────────────────────────
--   SELECT f.name AS file_name, t.*
--   FROM google_drive_list('1C01bJDPMZfChCJhgUd11W9eF3HVqXjYT') AS f,
--        LATERAL google_sheet_tabs(f.id) AS t
--   WHERE f.mime_type = 'application/vnd.google-apps.spreadsheet';
