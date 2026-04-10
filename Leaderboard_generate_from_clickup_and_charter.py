import argparse
import csv
import logging
from logging.handlers import RotatingFileHandler
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gspread
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from rapidfuzz import fuzz, process
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
DEFAULT_START_DATE = "2025-06-01"
DEFAULT_TIMEZONE_OFFSET = 6
DEFAULT_PAGE_SIZE = 100

CLICKUP_ACCEPTANCE_DATE_FIELD_ID = "dc88b3fc-e3ee-475a-8808-620bcf4f6676"
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass
class AppConfig:
    clickup_api_token: str
    clickup_list_id: str
    google_application_credentials: str
    delivery_sheet_key: str
    delivery_worksheet: str = "charter_stats"
    rating_worksheet: str = "rating_and_contribution"
    final_report_worksheet: str = "final_report"
    etl_logs_worksheet: str = "ETL_Logs"
    start_date: str = DEFAULT_START_DATE
    end_date: str = ""
    year_filter: Optional[int] = None
    page_size: int = DEFAULT_PAGE_SIZE
    time_zone_offset: int = DEFAULT_TIMEZONE_OFFSET
    log_level: str = "INFO"
    schedule_every_days: int = 3
    schedule_guard_enabled: bool = False
    force_run: bool = False
    clickup_timeout_seconds: int = 60
    google_retry_sleep_seconds: float = 1.0
    clickup_min_interval_seconds: float = 0.8
    gsheet_min_interval_seconds: float = 1.2
    gsheet_write_chunk_size: int = 500
    api_max_retries: int = 8

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv()
        raw: Dict[str, str] = dict(os.environ)
        start_date = raw.get("START_DATE", DEFAULT_START_DATE)
        year_filter_raw = raw.get("YEAR_FILTER", "").strip()
        return cls(
            clickup_api_token=_required_env(raw, "CLICKUP_API_TOKEN"),
            clickup_list_id=_required_env(raw, "CLICKUP_LIST_ID"),
            google_application_credentials=_required_env(raw, "GOOGLE_APPLICATION_CREDENTIALS"),
            delivery_sheet_key=_required_env(raw, "DELIVERY_SHEET_KEY"),
            delivery_worksheet=raw.get("DELIVERY_WORKSHEET", "charter_stats"),
            rating_worksheet=raw.get("RATING_WORKSHEET", "rating_and_contribution"),
            final_report_worksheet=raw.get("FINAL_REPORT_WORKSHEET", "final_report"),
            etl_logs_worksheet=raw.get("ETL_LOGS_WORKSHEET", "ETL_Logs"),
            start_date=start_date,
            end_date=raw.get("END_DATE", ""),
            year_filter=int(year_filter_raw) if year_filter_raw else _parse_iso_date(start_date).year,
            page_size=int(raw.get("PAGE_SIZE", str(DEFAULT_PAGE_SIZE))),
            time_zone_offset=int(raw.get("TIME_ZONE_OFFSET", str(DEFAULT_TIMEZONE_OFFSET))),
            log_level=raw.get("LOG_LEVEL", "INFO"),
            schedule_every_days=int(raw.get("SCHEDULE_EVERY_DAYS", "3")),
            schedule_guard_enabled=_parse_bool(raw.get("SCHEDULE_GUARD_ENABLED", "false")),
            force_run=_parse_bool(raw.get("FORCE_RUN", "false")),
            clickup_timeout_seconds=int(raw.get("CLICKUP_TIMEOUT_SECONDS", "60")),
            google_retry_sleep_seconds=float(raw.get("GOOGLE_RETRY_SLEEP_SECONDS", "1.0")),
            clickup_min_interval_seconds=float(raw.get("CLICKUP_MIN_INTERVAL_SECONDS", "0.8")),
            gsheet_min_interval_seconds=float(raw.get("GSHEET_MIN_INTERVAL_SECONDS", "1.2")),
            gsheet_write_chunk_size=int(raw.get("GSHEET_WRITE_CHUNK_SIZE", "500")),
            api_max_retries=int(raw.get("API_MAX_RETRIES", "8")),
        )

    def validate(self) -> None:
        _parse_iso_date(self.start_date)
        if self.end_date:
            _parse_iso_date(self.end_date)
        if self.schedule_every_days <= 0:
            raise ConfigError("SCHEDULE_EVERY_DAYS must be greater than 0.")
        if self.gsheet_write_chunk_size <= 0:
            raise ConfigError("GSHEET_WRITE_CHUNK_SIZE must be greater than 0.")
        if self.api_max_retries <= 0:
            raise ConfigError("API_MAX_RETRIES must be greater than 0.")
        credentials_path = (BASE_DIR / self.google_application_credentials).resolve() if not Path(
            self.google_application_credentials
        ).is_absolute() else Path(self.google_application_credentials)
        if not credentials_path.exists():
            raise ConfigError(
                f"Google credentials file not found at '{credentials_path}'."
            )

    @property
    def credentials_path(self) -> Path:
        path = Path(self.google_application_credentials)
        return path if path.is_absolute() else (BASE_DIR / path).resolve()

    @property
    def start_datetime(self) -> datetime:
        return datetime.combine(_parse_iso_date(self.start_date), datetime.min.time(), tzinfo=timezone.utc)

    @property
    def end_datetime(self) -> datetime:
        if self.end_date:
            return datetime.combine(_parse_iso_date(self.end_date), datetime.max.time(), tzinfo=timezone.utc)
        return datetime.now(tz=timezone.utc)


@dataclass
class RunSummary:
    fetched_tasks: int = 0
    accepted_tasks: int = 0
    existing_projects: int = 0
    new_projects_detected: int = 0
    processed_projects: int = 0
    skipped_projects: int = 0
    failed_projects: int = 0
    tracker_errors: int = 0
    failure_log_file: Optional[str] = None


@dataclass
class GoogleSheetTarget:
    sheet_key: str
    worksheet_name: str


@dataclass
class LogRecord:
    timestamp: str
    project_name: str
    status: str
    reason: str
    details: str = ""


@dataclass
class CharterPullResult:
    prod_ann_df: pd.DataFrame
    prod_qc_df: pd.DataFrame
    tracker_ann_df: pd.DataFrame
    tracker_qc_df: pd.DataFrame
    total_completion_hour: float = 0.0


class ProcessingLogger:
    """Keeps failure and skip records for export and summary."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.records: List[LogRecord] = []

    def log_skip(self, project_name: str, reason: str, details: str = "") -> None:
        self._add("SKIPPED", project_name, reason, details, logging.INFO)

    def log_error(self, project_name: str, reason: str, details: str = "") -> None:
        self._add("ERROR", project_name, reason, details, logging.ERROR)

    def log_tracker_error(self, project_name: str, reason: str, details: str = "") -> None:
        self._add("TRACKER_ERROR", project_name, reason, details, logging.WARNING)

    def _add(
        self,
        status: str,
        project_name: str,
        reason: str,
        details: str,
        level: int,
    ) -> None:
        record = LogRecord(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            project_name=project_name,
            status=status,
            reason=reason,
            details=details,
        )
        self.records.append(record)
        self.logger.log(level, "%s | %s | %s | %s", status, project_name, reason, details)

    def export_to_csv(self) -> Optional[str]:
        if not self.records:
            return None
        filename = BASE_DIR / f"{datetime.now().strftime('%Y-%m-%d_%H-%M')}_processing_failures.csv"
        with filename.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["timestamp", "project_name", "status", "reason", "details"],
            )
            writer.writeheader()
            for record in self.records:
                writer.writerow(record.__dict__)
        return str(filename)

    def summary_counts(self) -> Dict[str, int]:
        skipped = sum(record.status == "SKIPPED" for record in self.records)
        errors = sum(record.status == "ERROR" for record in self.records)
        tracker_errors = sum(record.status == "TRACKER_ERROR" for record in self.records)
        return {
            "total": len(self.records),
            "skipped": skipped,
            "errors": errors,
            "tracker_errors": tracker_errors,
        }

    def to_dataframe(self) -> pd.DataFrame:
        if not self.records:
            return pd.DataFrame(
                columns=["timestamp", "project_name", "status", "reason", "details"]
            )
        return pd.DataFrame([record.__dict__ for record in self.records])


class ClickUpClient:
    """Fetches tasks from ClickUp with pagination and retries."""

    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self._last_request_time = 0.0
        retry = Retry(
            total=config.api_max_retries,
            connect=config.api_max_retries,
            read=config.api_max_retries,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "Authorization": config.clickup_api_token,
                "Content-Type": "application/json",
            }
        )

    def fetch_all_tasks(self) -> List[Dict[str, Any]]:
        tasks: List[Dict[str, Any]] = []
        page = 0
        while True:
            self._wait_for_rate_limit()
            response = self.session.get(
                f"https://api.clickup.com/api/v2/list/{self.config.clickup_list_id}/task",
                params={
                    "page": page,
                    "limit": self.config.page_size,
                    "include_closed": True,
                    "include_archived": True,
                },
                timeout=self.config.clickup_timeout_seconds,
            )
            self._last_request_time = time.monotonic()
            response.raise_for_status()
            payload = response.json()
            page_tasks = payload.get("tasks", [])
            tasks.extend(page_tasks)
            self.logger.info("Fetched ClickUp page %s with %s tasks", page, len(page_tasks))
            if len(page_tasks) < self.config.page_size:
                break
            page += 1
        self.logger.info("Fetched %s total tasks from ClickUp", len(tasks))
        return tasks

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        wait_seconds = self.config.clickup_min_interval_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)


class GoogleSheetsClient:
    """Wrapper around gspread with small helpers for sheet operations."""

    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        credentials = Credentials.from_service_account_file(
            str(config.credentials_path),
            scopes=GOOGLE_SCOPES,
        )
        self.client = gspread.authorize(credentials)
        self.logger = logger
        self.config = config
        self._last_request_time = 0.0
        self._spreadsheet_cache: Dict[str, Any] = {}

    def worksheet(self, target: GoogleSheetTarget):
        spreadsheet = self._spreadsheet(target.sheet_key)
        return self._with_retry(lambda: spreadsheet.worksheet(target.worksheet_name))

    def existing_project_names(self, target: GoogleSheetTarget) -> List[str]:
        worksheet = self.worksheet(target)
        values = self._with_retry(worksheet.get_all_values)
        if len(values) <= 1:
            return []
        df = pd.DataFrame(values[1:], columns=values[0])
        if "Project Name" not in df.columns:
            return []
        return (
            df["Project Name"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda series: series.ne("")]
            .unique()
            .tolist()
        )

    def append_dataframe(self, target: GoogleSheetTarget, dataframe: pd.DataFrame) -> None:
        worksheet = self.worksheet(target)
        existing_rows = self._with_retry(worksheet.get_all_values)
        header = dataframe.columns.tolist()
        rows = dataframe.replace([np.nan, np.inf, -np.inf], "").values.tolist()
        if not rows:
            return

        if not existing_rows:
            self._update_in_chunks(worksheet, 1, [header] + rows)
            return

        first_row = existing_rows[0]
        if not any(str(cell).strip() for cell in first_row):
            self._update_in_chunks(worksheet, 1, [header] + rows)
            return

        if [str(cell).strip() for cell in first_row] != header:
            raise ValueError(
                f"Worksheet '{target.worksheet_name}' header does not match expected schema."
            )

        start_row = len(existing_rows) + 1
        try:
            self._update_in_chunks(worksheet, start_row, rows)
        except gspread.exceptions.APIError as exc:
            if "exceeds grid limits" not in str(exc):
                raise
            self._with_retry(lambda: worksheet.add_rows(max(len(rows), 100)))
            self._update_in_chunks(worksheet, start_row, rows)

    def replace_dataframe(self, target: GoogleSheetTarget, dataframe: pd.DataFrame) -> None:
        spreadsheet = self._spreadsheet(target.sheet_key)
        try:
            worksheet = self._with_retry(lambda: spreadsheet.worksheet(target.worksheet_name))
            self._with_retry(worksheet.clear)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self._with_retry(lambda: spreadsheet.add_worksheet(
                title=target.worksheet_name,
                rows=max(len(dataframe) + 100, 1000),
                cols=max(len(dataframe.columns) + 10, 50),
            ))
        rows = [dataframe.columns.tolist()] + dataframe.replace([np.nan, np.inf, -np.inf], "").values.tolist()
        self._update_in_chunks(worksheet, 1, rows)

    def _spreadsheet(self, sheet_key: str):
        if sheet_key not in self._spreadsheet_cache:
            self._spreadsheet_cache[sheet_key] = self._with_retry(lambda: self.client.open_by_key(sheet_key))
        return self._spreadsheet_cache[sheet_key]

    def _update_in_chunks(self, worksheet: Any, start_row: int, rows: List[List[Any]]) -> None:
        if not rows:
            return
        chunk_size = self.config.gsheet_write_chunk_size
        for chunk_start in range(0, len(rows), chunk_size):
            chunk = rows[chunk_start : chunk_start + chunk_size]
            row_number = start_row + chunk_start
            self._with_retry(
                lambda row_number=row_number, chunk=chunk: worksheet.update(
                    range_name=f"A{row_number}",
                    values=chunk,
                    value_input_option="USER_ENTERED",
                )
            )

    def _with_retry(self, operation):
        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.api_max_retries + 1):
            self._wait_for_rate_limit()
            try:
                result = operation()
                self._last_request_time = time.monotonic()
                return result
            except gspread.exceptions.APIError as exc:
                last_error = exc
                if not self._is_retryable_google_error(exc):
                    raise
                sleep_seconds = min(60.0, self.config.gsheet_min_interval_seconds * (2 ** (attempt - 1)))
                self.logger.warning(
                    "Google Sheets quota/backoff on attempt %s/%s; sleeping %.1fs",
                    attempt,
                    self.config.api_max_retries,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
            except requests.RequestException as exc:
                last_error = exc
                sleep_seconds = min(60.0, self.config.gsheet_min_interval_seconds * (2 ** (attempt - 1)))
                self.logger.warning(
                    "Google API transport retry on attempt %s/%s; sleeping %.1fs",
                    attempt,
                    self.config.api_max_retries,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
        if last_error:
            raise last_error
        raise RuntimeError("Google Sheets operation failed without a captured exception.")

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        wait_seconds = self.config.gsheet_min_interval_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    @staticmethod
    def _is_retryable_google_error(exc: Exception) -> bool:
        error_text = str(exc).lower()
        retry_markers = [
            "429",
            "quota",
            "rate limit",
            "user-rate limit",
            "resource exhausted",
            "internal error",
            "backend error",
            "503",
            "502",
            "500",
        ]
        return any(marker in error_text for marker in retry_markers)


class TextUtils:
    @staticmethod
    def extract_sheet_key(url: str) -> Optional[str]:
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url or "")
        return match.group(1) if match else None

    @staticmethod
    def normalize_text(text: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (text or "").lower())

    @staticmethod
    def fix_qai_id(raw_value: Any) -> Any:
        if pd.isna(raw_value):
            return raw_value
        value = str(raw_value).strip().replace(" ", "")
        value = re.sub(r"(?i)^QAI_?", "", value)
        match = re.match(r"([A-Za-z]+)(\d+)", value)
        if not match:
            return f"QAI_{value.upper()}"
        letters, digits = match.groups()
        return f"QAI_{letters.upper()}{digits}"

    @staticmethod
    def extract_numeric_value(raw_value: Any) -> Any:
        if raw_value in (None, ""):
            return ""
        match = re.search(r"\d+", str(raw_value))
        return float(match.group()) if match else ""


class ColumnMapper:
    ALIAS_DICT = {
        "Resource Allocation": ["Resource Allocation"],
        "Resource Type": ["Designation", "Resource Type", "Resource type"],
        "Full Name": ["Full Name", "Name", "Annotator Name"],
        "QAI ID": ["QAI ID", "QAI", "Annotator ID", "Labeler ID"],
        "Final Working Hour": ["Final Working Hour", "Total Hour", "Final Hour"],
        "Effective Hour": ["Effective Hour", "Effort Hour"],
        "Bonus": ["Bonus", "Additional Hour"],
        "Penalty": ["Penalty", "Deduction"],
        "Rating": ["Ratings", "Rating", "Rating ", " Rating ", " Rating"],
        "No. Of Task": ["No. of Task", "Number of Task", "Total Task", "Task Count", "Tasks", "No Of Task"],
        "No. Of Incorrect Task": [
            "No Of Incorrect Task",
            "Number of Incorrect Task",
            "Incorrect Task",
            "Incorrect Tasks",
            "No Of In Correct Task",
            "No. Of Wrong Tasks",
        ],
        "No. of QA Task": ["No. of QA Task", "QA Task"],
        "No. of CC Task": ["No. of CC Task", "CC Task"],
        "Accuracy": ["Accuracy", "Accuracy (%)"],
    }

    @classmethod
    def get_fuzzy_column_map(
        cls,
        headers: List[str],
        target_columns: List[str],
        threshold: int = 75,
    ) -> Dict[str, str]:
        column_map: Dict[str, str] = {}
        for target in target_columns:
            candidates = cls.ALIAS_DICT.get(target, [target])
            best_match: Optional[str] = None
            best_score = 0
            for candidate in candidates:
                match = process.extractOne(candidate, headers, scorer=fuzz.token_sort_ratio)
                if not match:
                    continue
                matched_header, score, _ = match
                if score > best_score:
                    best_match = matched_header
                    best_score = score
            if best_match and best_score >= threshold:
                column_map[target] = best_match
        return column_map


class CharterDataExtractor:
    def __init__(self, sheets: GoogleSheetsClient, processing_logger: ProcessingLogger, logger: logging.Logger) -> None:
        self.sheets = sheets
        self.client = sheets.client
        self.processing_logger = processing_logger
        self.logger = logger

    def pull_charter_stats(self, project_name: str, charter_url: str) -> Optional[CharterPullResult]:
        if not str(charter_url or "").strip():
            return None
        sheet_key = TextUtils.extract_sheet_key(charter_url)
        if not sheet_key:
            self.processing_logger.log_error(project_name, "Invalid charter URL", charter_url)
            return None
        try:
            spreadsheet = self.client.open_by_key(sheet_key)
            worksheet_map = {ws.title.strip().lower(): ws for ws in spreadsheet.worksheets()}
            dashboard = worksheet_map.get("dashboard")
            if not dashboard:
                self.processing_logger.log_error(project_name, "Dashboard worksheet missing")
                return None
            all_data = dashboard.get_all_values()
            sections, total_completion_hour = self._parse_sections(all_data)
            prod_ann = self._process_section(project_name, all_data, sections, "Production: Annotation")
            prod_qc = self._process_section(project_name, all_data, sections, "Production: Quality Check")
            tracker_ann = self._process_section(project_name, all_data, sections, "Task Tracker (Annotation)")
            tracker_qc = self._process_section(project_name, all_data, sections, "Task Tracker (Quality Check)")
            return CharterPullResult(
                prod_ann_df=prod_ann if prod_ann is not None else pd.DataFrame(),
                prod_qc_df=prod_qc if prod_qc is not None else pd.DataFrame(),
                tracker_ann_df=tracker_ann if tracker_ann is not None else pd.DataFrame(),
                tracker_qc_df=tracker_qc if tracker_qc is not None else pd.DataFrame(),
                total_completion_hour=total_completion_hour,
            )
        except Exception as exc:
            self.processing_logger.log_error(project_name, "Failed to pull charter", str(exc))
            return None

    def _parse_sections(self, all_data: List[List[str]]) -> Tuple[Dict[str, Dict[str, int]], float]:
        sections: Dict[str, Dict[str, int]] = {}
        current_section: Optional[str] = None
        total_completion_hour = 0.0
        for index, row in enumerate(all_data):
            if not row:
                continue
            cell = row[0].strip()
            if "Number of Labels" in cell:
                continue
            if cell in {"Production: Annotation", "Production: Quality Check"}:
                if current_section:
                    sections[current_section]["end"] = index
                current_section = cell
                sections[current_section] = {"start": index + 2}
            elif cell == "Task Tracker ( Annotation)":
                if current_section:
                    sections[current_section]["end"] = index
                current_section = "Task Tracker (Annotation)"
                sections[current_section] = {"start": index + 2}
            elif cell in {"Task Tracker ( Quality Check )", "Task Tracker ( Quality Check)"}:
                if current_section:
                    sections[current_section]["end"] = index
                current_section = "Task Tracker (Quality Check)"
                sections[current_section] = {"start": index + 2}
            elif cell == "Internal: Task Tracker Sheet":
                if current_section:
                    sections[current_section]["end"] = index
                current_section = "Task Tracker (Annotation)"
                sections[current_section] = {"start": index + 2}
            elif "Total Completion hour" in cell:
                for value in row[1:]:
                    if str(value).strip():
                        total_completion_hour = float(value)
                        break
            elif current_section and (cell == "Total" or cell == ""):
                sections[current_section]["end"] = index
        if current_section and "end" not in sections[current_section]:
            sections[current_section]["end"] = len(all_data)
        return sections, total_completion_hour

    def _process_section(
        self,
        project_name: str,
        all_data: List[List[str]],
        sections: Dict[str, Dict[str, int]],
        section_name: str,
    ) -> Optional[pd.DataFrame]:
        bounds = sections.get(section_name)
        if not bounds:
            return pd.DataFrame()
        headers = [header.strip() for header in all_data[bounds["start"] - 1]]
        section_rows = all_data[bounds["start"] : bounds["end"]]
        filtered_rows = [row for row in section_rows if len(row) >= 2 and any(str(cell).strip() for cell in row)]
        if not filtered_rows:
            self.processing_logger.log_skip(project_name, f"No rows found in {section_name}")
            return pd.DataFrame()
        source_columns = self._source_columns(section_name)
        column_map = ColumnMapper.get_fuzzy_column_map(headers, source_columns)
        normalized_rows: List[List[Any]] = []
        for row in filtered_rows:
            normalized_row: List[Any] = []
            for source_column in source_columns:
                header = column_map.get(source_column)
                if not header or header not in headers:
                    normalized_row.append("")
                    continue
                column_index = headers.index(header)
                normalized_row.append(row[column_index] if column_index < len(row) else "")
            normalized_rows.append(normalized_row)
        dataframe = pd.DataFrame(normalized_rows, columns=source_columns)
        if "QAI ID" in dataframe.columns:
            dataframe["QAI ID"] = dataframe["QAI ID"].apply(TextUtils.fix_qai_id)
        if "Final Working Hour" in dataframe.columns:
            dataframe["Final Working Hour"] = pd.to_numeric(dataframe["Final Working Hour"], errors="coerce").fillna(0)
        if "QAI ID" in dataframe.columns and "Final Working Hour" in dataframe.columns:
            dataframe = dataframe[
                ~(
                    dataframe["QAI ID"].astype(str).str.strip().eq("")
                    & dataframe["Final Working Hour"].eq(0)
                )
            ].copy()
        if section_name == "Production: Annotation":
            dataframe["Type"] = "Annotation"
        elif section_name == "Production: Quality Check":
            dataframe["Type"] = "QC"
        elif section_name.startswith("Task Tracker"):
            dataframe["Type"] = "Tracking"
        return dataframe.drop_duplicates().reset_index(drop=True)

    @staticmethod
    def _source_columns(section_name: str) -> List[str]:
        if section_name == "Task Tracker (Annotation)":
            return [
                "Resource Allocation",
                "Resource Type",
                "Full Name",
                "QAI ID",
                "Final Working Hour",
                "No. Of Task",
                "No. Of Incorrect Task",
            ]
        if section_name == "Task Tracker (Quality Check)":
            return [
                "Resource Allocation",
                "Resource Type",
                "Full Name",
                "QAI ID",
                "Final Working Hour",
                "No. of QA Task",
                "No. Of Incorrect Task",
                "No. of CC Task",
            ]
        return [
            "Resource Allocation",
            "Resource Type",
            "Full Name",
            "QAI ID",
            "Effective Hour",
            "Bonus",
            "Penalty",
            "Final Working Hour",
            "Accuracy",
            "Rating",
        ]


class AccuracyTracker:
    def __init__(self, sheets: GoogleSheetsClient, processing_logger: ProcessingLogger) -> None:
        self.client = sheets.client
        self.processing_logger = processing_logger

    def get_accuracy(self, project_name: str, tracker_link: str) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        try:
            spreadsheet = self.client.open_by_url(tracker_link)
            dashboard = spreadsheet.worksheet("Dashboard")
            values = dashboard.get_all_values()
            if len(values) <= 1:
                self.processing_logger.log_tracker_error(project_name, "Tracker dashboard is empty")
                return None
            df = pd.DataFrame(values[1:], columns=values[0])
            annotator_df = df.iloc[:, 0:6].copy()
            reviewer_df = df.iloc[:, 8:16].copy()
            annotator_df.columns = annotator_df.columns.str.strip()
            reviewer_df.columns = reviewer_df.columns.str.strip()
            annotator_df = annotator_df[
                ~annotator_df["Annotator ID"].astype(str).str.strip().isin(["", "Total"])
            ].reset_index(drop=True)
            reviewer_df = reviewer_df[
                ~reviewer_df["Reviewer ID"].astype(str).str.strip().isin(["", "Total"])
            ].reset_index(drop=True)
            annotator_accuracy = dict(zip(annotator_df["Annotator ID"], annotator_df["Accuracy (%)"]))
            reviewer_accuracy = dict(zip(reviewer_df["Reviewer ID"], reviewer_df["Accuracy (%)"]))
            return annotator_accuracy, reviewer_accuracy
        except Exception as exc:
            self.processing_logger.log_tracker_error(project_name, "Failed to read tracker", str(exc))
            return None


class ClickUpFieldExtractor:
    def extract(self, task_json: Dict[str, Any]) -> Dict[str, Any]:
        project_name = task_json.get("name", "")
        return {
            "Project Name": project_name,
            "Project Batch": self._extract_batch(project_name),
            "Ticket Creation Date": self._extract_date_created(task_json.get("date_created")),
            "Month": self._extract_month(task_json.get("date_created")),
            "Start Date": self._parse_date_field(self._get_custom_field(task_json, "Start Date")),
            "Acceptance Date": self._parse_date_field(self._get_custom_field(task_json, "Acceptance Date ")),
            "Client Alias": (self._get_custom_field(task_json, "Client Alias") or {}).get("value", ""),
            "Industry Type": self._parse_dropdown_field(self._get_custom_field(task_json, "Industry type")),
            "PDL": self._parse_users_field(self._get_custom_field(task_json, "PDL")),
            "Delivery Lead": self._parse_delivery_lead(task_json, "Delivery Lead"),
            "Data Type": self._parse_dropdown_field(self._get_custom_field(task_json, "Data Type")),
            "Labeling Tool": self._parse_labeling_tool(self._get_custom_field_with_value(task_json, "Labeling Tool")),
            "Platform": self._parse_platform_field(self._get_custom_field(task_json, "Platform")),
            "Charter Link": self._get_field_link(task_json, "Project Charter Link"),
            "Task Tracker Link": self._get_field_link(task_json, "Task/Progress Tracker"),
            "PDR": TextUtils.extract_numeric_value(
                self._parse_dropdown_field(
                    self._get_custom_field(task_json, "Project Difficulty Rating (PDR)")
                )
            ),
            "Feedback(Client)": self._get_feedback_client(task_json),
        }

    def _get_custom_field(self, task_json: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
        for field in task_json.get("custom_fields", []):
            if field.get("name", "").strip().lower() == name.strip().lower():
                return field
        return None

    def _get_custom_field_with_value(self, task_json: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
        fields = [
            field
            for field in task_json.get("custom_fields", [])
            if field.get("name", "").strip().lower() == name.strip().lower()
        ]
        for field in fields:
            if "value" in field:
                return field
        return fields[0] if fields else None

    def _parse_delivery_lead(self, task_json: Dict[str, Any], name: str) -> str:
        field = self._get_custom_field(task_json, name)
        return str(field.get("value", "")) if field else ""

    @staticmethod
    def _parse_dropdown_field(field: Optional[Dict[str, Any]]) -> Optional[str]:
        if not field or "value" not in field:
            return None
        value = field.get("value", "")
        for option in field.get("type_config", {}).get("options", []):
            if option.get("orderindex") == value:
                return option.get("name")
        return None

    @staticmethod
    def _parse_users_field(field: Optional[Dict[str, Any]]) -> Optional[str]:
        if not field or "value" not in field:
            return None
        values = [user.get("username") for user in field.get("value", [])]
        return ", ".join(map(str, values)) if values else None

    @staticmethod
    def _parse_date_field(field: Optional[Dict[str, Any]]) -> Optional[str]:
        if not field or "value" not in field:
            return None
        return datetime.fromtimestamp(int(field["value"]) / 1000).strftime("%Y-%m-%d")

    def _get_field_link(self, task_json: Dict[str, Any], field_name: str) -> str:
        field = self._get_custom_field(task_json, field_name)
        return field.get("value", "") if field else ""

    def _parse_platform_field(self, field: Optional[Dict[str, Any]]) -> str:
        try:
            if not field:
                return ""
            value = field.get("value", "")
            return field["type_config"]["options"][value]["name"] if value else ""
        except Exception:
            return ""

    def _parse_labeling_tool(self, field: Optional[Dict[str, Any]]) -> str:
        try:
            if not field:
                return ""
            value = field.get("value", "")
            return field["type_config"]["options"][value]["name"] if value else ""
        except Exception:
            return ""

    def _get_feedback_client(self, task_json: Dict[str, Any]) -> Any:
        field = self._get_custom_field_with_value(task_json, "Feedback (Client)")
        if not field:
            return ""
        field_type = field.get("type")
        if field_type == "drop_down":
            return self._parse_dropdown_field(field) or ""
        if field_type == "users":
            values = field.get("value") or []
            return ", ".join(user.get("username", "") for user in values if user.get("username"))
        return field.get("value", "") or ""

    @staticmethod
    def _extract_batch(project_name: str) -> str:
        match = re.search(r"(Batch_\d+)", project_name or "")
        return match.group(1) if match else ""

    @staticmethod
    def _extract_date_created(raw_timestamp: Optional[str]) -> str:
        if not raw_timestamp:
            return ""
        return datetime.fromtimestamp(int(raw_timestamp) / 1000).strftime("%Y-%m-%d")

    @staticmethod
    def _extract_month(raw_timestamp: Optional[str]) -> str:
        if not raw_timestamp:
            return ""
        return datetime.fromtimestamp(int(raw_timestamp) / 1000).strftime("%B")


class DataExporter:
    def __init__(
        self,
        sheets: GoogleSheetsClient,
        delivery_target: GoogleSheetTarget,
        rating_target: GoogleSheetTarget,
        processing_logger: ProcessingLogger,
    ) -> None:
        self.sheets = sheets
        self.delivery_target = delivery_target
        self.rating_target = rating_target
        self.processing_logger = processing_logger

    def export(self, project_name: str, charter_dataframes: List[pd.DataFrame]) -> None:
        prepared = [df.drop_duplicates().reset_index(drop=True) for df in charter_dataframes if not df.empty]
        if not prepared:
            self.processing_logger.log_skip(project_name, "Charter is empty")
            return
        delivery_df = pd.concat(prepared, ignore_index=True).replace([np.nan, np.inf, -np.inf], "")
        rating_df = self._build_rating_dataframe(delivery_df)
        self.sheets.append_dataframe(self.delivery_target, delivery_df.drop(columns=["Rating"], errors="ignore"))
        self.sheets.append_dataframe(self.rating_target, rating_df)

    @staticmethod
    def _build_rating_dataframe(complete_charter_data: pd.DataFrame) -> pd.DataFrame:
        df = complete_charter_data[["Project Name", "QAI ID", "Final Working Hour", "Rating"]].copy()
        df["Final Working Hour"] = pd.to_numeric(df["Final Working Hour"], errors="coerce").fillna(0)
        grouped = df.groupby(["Project Name", "QAI ID"], as_index=False).agg(
            {"Final Working Hour": "sum", "Rating": "first"}
        )
        totals = grouped.groupby("Project Name")["Final Working Hour"].sum().to_dict()
        grouped["Contribution%"] = grouped.apply(
            lambda row: round(
                (row["Final Working Hour"] / totals.get(row["Project Name"], 1) * 100), 2
            )
            if totals.get(row["Project Name"], 0) > 0
            else 0,
            axis=1,
        )
        return grouped[["Project Name", "QAI ID", "Rating", "Final Working Hour", "Contribution%"]]


class FinalReportGenerator:
    def __init__(self, sheets: GoogleSheetsClient, logger: logging.Logger) -> None:
        self.sheets = sheets
        self.logger = logger

    def generate(
        self,
        source_target: GoogleSheetTarget,
        output_target: GoogleSheetTarget,
    ) -> None:
        worksheet = self.sheets.worksheet(source_target)
        values = worksheet.get_all_values()
        if len(values) <= 1:
            self.logger.warning("No data found in worksheet '%s'", source_target.worksheet_name)
            return
        df = pd.DataFrame(values[1:], columns=values[0])
        original_columns = [column for column in df.columns if column != "Type"]
        numeric_columns = ["Effective Hour", "Bonus", "Penalty", "Final Working Hour"]
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
        results: List[Dict[str, Any]] = []
        for (_, qai_id), group in df.groupby(["Project Name", "QAI ID"]):
            if not qai_id or str(qai_id).strip() == "":
                continue
            row: Dict[str, Any] = {}
            for column in original_columns:
                if column == "Accuracy":
                    ann_qc_group = group[group["Type"].isin(["Annotation", "QC"])]
                    ann_row = ann_qc_group[ann_qc_group["Type"] == "Annotation"]
                    qc_row = ann_qc_group[ann_qc_group["Type"] == "QC"]
                    ann_acc = ann_row["Accuracy"].iloc[0] if not ann_row.empty else None
                    qc_acc = qc_row["Accuracy"].iloc[0] if not qc_row.empty else None
                    row[column] = self._calculate_accuracy(ann_acc, qc_acc)
                elif column in numeric_columns:
                    row[column] = group[column].sum()
                else:
                    row[column] = group[column].iloc[0] if not group[column].empty else ""
            results.append(row)
        output_df = pd.DataFrame(results)
        if output_df.empty:
            self.logger.warning("Final report dataset is empty after aggregation")
            return
        output_df = output_df[original_columns].replace([np.nan, np.inf, -np.inf], "")
        self.sheets.replace_dataframe(output_target, output_df)
        self.logger.info("Generated final report with %s rows", len(output_df))

    @staticmethod
    def _calculate_accuracy(annotation_acc: Any, qc_acc: Any) -> float:
        def clean(value: Any) -> float:
            if value is None or str(value).strip() == "":
                return 100.0
            cleaned = str(value).replace("%", "").strip()
            try:
                return float(cleaned)
            except ValueError:
                return 100.0

        return (clean(annotation_acc) + clean(qc_acc)) / 2 / 100


class ProjectProcessor:
    DF_ORDERS = [
        "Project Name",
        "Ticket Creation Date",
        "Month",
        "Start Date",
        "Acceptance Date",
        "Client Alias",
        "Industry Type",
        "PDL",
        "Delivery Lead",
        "Data Type",
        "Labeling Tool",
        "Platform",
        "Charter Link",
        "Task Tracker Link",
        "Resource Allocation",
        "Resource Type",
        "Full Name",
        "QAI ID",
        "Effective Hour",
        "Bonus",
        "Penalty",
        "Final Working Hour",
        "Accuracy",
        "Rating",
        "Type",
        "PDR",
        "Feedback(Client)",
    ]
    SKIP_PATTERNS = ["Aquabyte", "QS Radio", "QAI_Nets Game", "Demo"]

    def __init__(
        self,
        field_extractor: ClickUpFieldExtractor,
        charter_extractor: CharterDataExtractor,
        accuracy_tracker: AccuracyTracker,
        data_exporter: DataExporter,
        processing_logger: ProcessingLogger,
        logger: logging.Logger,
    ) -> None:
        self.field_extractor = field_extractor
        self.charter_extractor = charter_extractor
        self.accuracy_tracker = accuracy_tracker
        self.data_exporter = data_exporter
        self.processing_logger = processing_logger
        self.logger = logger

    def process(
        self,
        task: Dict[str, Any],
        existing_projects: Iterable[str],
        year_filter: int,
    ) -> bool:
        project_info = self.field_extractor.extract(task)
        project_name = project_info.get("Project Name", "Unknown")
        try:
            if project_name in existing_projects:
                self.processing_logger.log_skip(project_name, "Project already exists in destination sheet")
                return False
            if self._should_skip_project(project_name):
                self.processing_logger.log_skip(project_name, "Dashboard format is intentionally skipped")
                return False
            charter_link = str(project_info.get("Charter Link", "") or "").strip()
            if not charter_link:
                self.processing_logger.log_skip(project_name, "No Charter Link")
                return False
            charter_result = self.charter_extractor.pull_charter_stats(
                project_name=project_name,
                charter_url=charter_link,
            )
            if not charter_result:
                return False
            charter_dataframes = [
                charter_result.prod_ann_df.copy(),
                charter_result.prod_qc_df.copy(),
                charter_result.tracker_ann_df.copy(),
                charter_result.tracker_qc_df.copy(),
            ]
            if all(df.empty for df in charter_dataframes):
                self.processing_logger.log_skip(project_name, "No Charter Data")
                return False
            charter_dataframes = self._enrich_dataframes(charter_dataframes, project_info)
            prod_ann_df, prod_qc_df, _, _ = charter_dataframes
            if self._should_fetch_tracker_accuracy(
                project_info.get("Acceptance Date"),
                year_filter,
                self._has_blank_accuracy(prod_ann_df),
            ):
                self._apply_tracker_accuracy(
                    project_name=project_name,
                    tracker_link=project_info.get("Task Tracker Link", ""),
                    prod_ann_df=prod_ann_df,
                    prod_qc_df=prod_qc_df,
                )
            self.data_exporter.export(project_name, charter_dataframes)
            self.logger.info("Processed project '%s'", project_name)
            return True
        except Exception as exc:
            self.processing_logger.log_error(project_name, "Exception during processing", str(exc))
            return False

    def _enrich_dataframes(
        self,
        dataframes: List[pd.DataFrame],
        project_info: Dict[str, Any],
    ) -> List[pd.DataFrame]:
        enriched: List[pd.DataFrame] = []
        for dataframe in dataframes:
            if dataframe.empty:
                enriched.append(dataframe)
                continue
            copy_df = dataframe.copy()
            for key, value in project_info.items():
                copy_df[key] = "" if value is None else str(value)
            copy_df["QAI ID"] = copy_df["QAI ID"].apply(TextUtils.fix_qai_id)
            copy_df = copy_df.reindex(columns=self.DF_ORDERS).fillna("")
            enriched.append(copy_df)
        return enriched

    def _apply_tracker_accuracy(
        self,
        project_name: str,
        tracker_link: str,
        prod_ann_df: pd.DataFrame,
        prod_qc_df: pd.DataFrame,
    ) -> None:
        if not tracker_link:
            self.processing_logger.log_tracker_error(project_name, "Missing Task Tracker Link")
            return
        accuracy = self.accuracy_tracker.get_accuracy(project_name, tracker_link)
        if not accuracy:
            return
        annotator_accuracy, reviewer_accuracy = accuracy
        if not prod_ann_df.empty:
            prod_ann_df["Accuracy"] = prod_ann_df["QAI ID"].map(annotator_accuracy).fillna(prod_ann_df["Accuracy"])
        if not prod_qc_df.empty:
            prod_qc_df["Accuracy"] = prod_qc_df["QAI ID"].map(reviewer_accuracy).fillna(prod_qc_df["Accuracy"])

    @classmethod
    def _should_skip_project(cls, project_name: str) -> bool:
        normalized = TextUtils.normalize_text(project_name)
        return any(TextUtils.normalize_text(pattern) in normalized for pattern in cls.SKIP_PATTERNS)

    @staticmethod
    def _has_blank_accuracy(dataframe: pd.DataFrame) -> bool:
        return (
            "Accuracy" not in dataframe.columns
            or dataframe["Accuracy"].isna().all()
            or dataframe["Accuracy"].astype(str).str.strip().eq("").all()
        )

    @staticmethod
    def _should_fetch_tracker_accuracy(
        acceptance_date_str: Optional[str],
        year_filter: int,
        blank_accuracy: bool,
    ) -> bool:
        if blank_accuracy:
            return True
        if not acceptance_date_str:
            return False
        try:
            return datetime.strptime(acceptance_date_str, "%Y-%m-%d").year == year_filter
        except ValueError:
            return False


def setup_logging(level_name: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("leaderboard_etl")
    logger.setLevel(getattr(logging, level_name.upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        LOG_DIR / f"generate_stats_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def filter_tasks_by_acceptance_date(
    tasks: List[Dict[str, Any]],
    start_date_utc: datetime,
    end_date_utc: datetime,
    timezone_offset_hours: int,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for task in tasks:
        status = task.get("status", {}).get("status", "").strip().lower()
        if status not in {"project accepted", "closed"}:
            continue
        acceptance_date = _extract_custom_field(task, CLICKUP_ACCEPTANCE_DATE_FIELD_ID, timezone_offset_hours, is_date=True)
        if acceptance_date and start_date_utc <= acceptance_date <= end_date_utc:
            filtered.append(task)
    return filtered


def _extract_custom_field(
    task: Dict[str, Any],
    field_id: str,
    timezone_offset_hours: int,
    is_date: bool = False,
) -> Any:
    for field in task.get("custom_fields", []):
        if field.get("id") != field_id:
            continue
        value = field.get("value")
        if is_date and value:
            utc_time = datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
            return utc_time + timedelta(hours=timezone_offset_hours)
        return value
    return None


def should_run_today(config: AppConfig, logger: logging.Logger) -> bool:
    if config.force_run or not config.schedule_guard_enabled:
        return True
    start = _parse_iso_date(config.start_date)
    today = datetime.now(tz=timezone.utc).date()
    day_delta = (today - start).days
    if day_delta < 0:
        logger.info(
            "Schedule guard skipped run because start date %s is in the future relative to %s",
            start.isoformat(),
            today.isoformat(),
        )
        return False
    if day_delta % config.schedule_every_days != 0:
        logger.info(
            "Schedule guard skipped run because %s days since %s is not divisible by %s",
            day_delta,
            start.isoformat(),
            config.schedule_every_days,
        )
        return False
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Leaderboard ETL from ClickUp to Google Sheets")
    parser.add_argument("--start-date", help="Override START_DATE in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="Override END_DATE in YYYY-MM-DD format")
    parser.add_argument("--force-run", action="store_true", help="Ignore schedule guard")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = AppConfig.from_env()
        if args.start_date:
            config.start_date = args.start_date
        if args.end_date:
            config.end_date = args.end_date
        if args.force_run:
            config.force_run = True
        config.validate()
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    logger = setup_logging(config.log_level)
    logger.info("Starting leaderboard ETL")
    logger.info(
        "Run window: %s to %s",
        config.start_datetime.isoformat(),
        config.end_datetime.isoformat(),
    )

    if not should_run_today(config, logger):
        logger.info("Run skipped by schedule guard")
        return 0

    processing_logger = ProcessingLogger(logger)
    summary = RunSummary()
    sheets: Optional[GoogleSheetsClient] = None
    etl_logs_target: Optional[GoogleSheetTarget] = None
    try:
        clickup_client = ClickUpClient(config, logger)
        sheets = GoogleSheetsClient(config, logger)
        delivery_target = GoogleSheetTarget(config.delivery_sheet_key, config.delivery_worksheet)
        rating_target = GoogleSheetTarget(config.delivery_sheet_key, config.rating_worksheet)
        final_report_target = GoogleSheetTarget(config.delivery_sheet_key, config.final_report_worksheet)
        etl_logs_target = GoogleSheetTarget(config.delivery_sheet_key, config.etl_logs_worksheet)

        tasks = clickup_client.fetch_all_tasks()
        summary.fetched_tasks = len(tasks)
        accepted_tasks = filter_tasks_by_acceptance_date(
            tasks,
            config.start_datetime,
            config.end_datetime,
            config.time_zone_offset,
        )
        summary.accepted_tasks = len(accepted_tasks)
        logger.info("Accepted tasks in date range: %s", len(accepted_tasks))

        existing_projects = sheets.existing_project_names(delivery_target)
        existing_project_set = set(existing_projects)
        summary.existing_projects = len(existing_projects)
        logger.info("Existing projects already in destination sheet: %s", len(existing_projects))

        field_extractor = ClickUpFieldExtractor()
        charter_extractor = CharterDataExtractor(sheets, processing_logger, logger)
        accuracy_tracker = AccuracyTracker(sheets, processing_logger)
        data_exporter = DataExporter(sheets, delivery_target, rating_target, processing_logger)
        project_processor = ProjectProcessor(
            field_extractor=field_extractor,
            charter_extractor=charter_extractor,
            accuracy_tracker=accuracy_tracker,
            data_exporter=data_exporter,
            processing_logger=processing_logger,
            logger=logger,
        )

        new_project_names: List[str] = []
        for task in accepted_tasks:
            project_name = field_extractor.extract(task).get("Project Name", "Unknown")
            if project_name not in existing_project_set:
                new_project_names.append(project_name)
        summary.new_projects_detected = len(new_project_names)
        logger.info("New projects detected: %s", summary.new_projects_detected)

        for index, task in enumerate(accepted_tasks, start=1):
            project_name = field_extractor.extract(task).get("Project Name", "Unknown")
            logger.info(
                "Processing project %s/%s: %s",
                index,
                len(accepted_tasks),
                project_name,
            )
            processed = project_processor.process(task, existing_project_set, config.year_filter)
            if processed:
                summary.processed_projects += 1
                existing_project_set.add(project_name)
            time.sleep(config.google_retry_sleep_seconds)

        final_report_generator = FinalReportGenerator(sheets, logger)
        final_report_generator.generate(delivery_target, final_report_target)

        failure_log = processing_logger.export_to_csv()
        summary.failure_log_file = failure_log
        sheets.replace_dataframe(etl_logs_target, processing_logger.to_dataframe())
        counts = processing_logger.summary_counts()
        summary.skipped_projects = counts["skipped"]
        summary.failed_projects = counts["errors"]
        summary.tracker_errors = counts["tracker_errors"]

        logger.info(
            "ETL complete | fetched=%s accepted=%s existing=%s new=%s processed=%s skipped=%s errors=%s tracker_errors=%s failure_log=%s",
            summary.fetched_tasks,
            summary.accepted_tasks,
            summary.existing_projects,
            summary.new_projects_detected,
            summary.processed_projects,
            summary.skipped_projects,
            summary.failed_projects,
            summary.tracker_errors,
            summary.failure_log_file or "none",
        )
        return 0
    except Exception:
        logger.exception("Leaderboard ETL failed unexpectedly")
        failure_log = processing_logger.export_to_csv()
        try:
            if sheets and etl_logs_target:
                sheets.replace_dataframe(etl_logs_target, processing_logger.to_dataframe())
        except Exception:
            logger.exception("Failed to export ETL logs worksheet")
        if failure_log:
            logger.info("Failure details exported to %s", failure_log)
        return 1


def _required_env(raw: Dict[str, str], key: str) -> str:
    value = raw.get(key, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {key}")
    return value


def _parse_bool(raw_value: str) -> bool:
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_iso_date(raw_value: str) -> date:
    return datetime.strptime(raw_value, "%Y-%m-%d").date()


if __name__ == "__main__":
    sys.exit(main())
