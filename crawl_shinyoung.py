# -*- coding: utf-8 -*-
# crawl_shinyoung.py
# 신영부동산신탁 공매 공지 → Google Sheets append
# - 교보 로직과 동일한 상태관리/중복 규칙/재시작 안전성/페이지내 역순 처리 유지
# - 목록에서 (번호/제목/게시일/URL) 파싱
# - 제목 기반 파생 필드: 주소/도시명/건물명/매각내용/용도(오피스텔)

import os, re, time, json, logging
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

from address_only_regex import (
  extract_address,
  extract_province_sgg,
  extract_building_name,
  extract_sale_content,
)

# ────────── Google Sheets 설정 ──────────
SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"  # 동일 스프레드시트
WORKSHEET_NAME   = "신영부동산_신탁"   # 워크시트명만 변경
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "번호","신탁사명","게시글 제목","게시일",
  "주소 (지번)","도시명","건물명","매각내용","용도",
  "중복여부","원문 링크(URL)"
]

# ────────── 크롤링 설정 ──────────
BASE = "http://www.shinyoungret.com"
LIST_PATH = "/"
TRUST_NAME = "신영부동산신탁"

DATE_RE = re.compile(r"\d{4}[.\-]\d{2}[.\-]\d{2}")
DELAY_SEC = 1.0

# 페이지 진행 방향: 예) 현재→과거(START_PAGE=1, END_PAGE=N, STEP=1) 또는 과거→현재(STEP=-1)
# 페이지 내에서는 역순 처리하여 전체 오름차순 업로드 보장
START_PAGE = 67
END_PAGE   = 1
STEP       = -1

RESUME = True
STATE_FILE = "output/.shinyoung_state.json"

# ────────── 시트/컬럼 유틸 ──────────
ADDRESS_COL_CANDIDATES  = ["address", "주소 (지번)", "주소", "Address"]
DUP_COL_CANDIDATES      = ["duplicate", "중복여부", "중복", "Duplicate"]
URL_COL_CANDIDATES      = ["url", "원문 링크(URL)", "링크", "URL"]

def _find_col_index(header: list, candidates: list) -> int:
  name_to_idx = {name: i for i, name in enumerate(header)}
  for c in candidates:
    if c in name_to_idx:
      return name_to_idx[c]
  return -1

def _col_letter(n: int) -> str:
  s = ""
  while n:
    n, r = divmod(n - 1, 26)
    s = chr(r + 65) + s
  return s

# ────────── 로깅/세션 ──────────
def setup_logging():
  os.makedirs("logs", exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  logfile = f"logs/shinyoung_sheets_{ts}.log"
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler()],
  )
  logging.info("로그 시작: %s", logfile)

def make_session() -> requests.Session:
  s = requests.Session()
  s.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": BASE,
  })
  retries = Retry(
    total=3,
    backoff_factor=0.6,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False,
  )
  adapter = HTTPAdapter(max_retries=retries)
  s.mount("http://", adapter)
  s.mount("https://", adapter)
  return s

# ────────── 시트 유틸 ──────────
def _open_sheet():
  creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=SCOPES)
  logging.info("Using service account: %s", creds.service_account_email)
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(SPREADSHEET_ID)
  try:
    ws = sh.worksheet(WORKSHEET_NAME)
  except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")
  existing = ws.get_all_values()
  if not existing:
    ws.append_row(HEADER)
  else:
    if existing[0] != HEADER:
      logging.warning("시트 헤더가 코드 정의와 다릅니다. (유연 매칭으로 진행)")
  return ws

def _normalize_duplicate_numbering(ws):
  """같은 주소가 2건 이상인 그룹만 '중복1..N'로 채우고, 1건뿐이면 공란 유지."""
  values = ws.get_all_values()
  if not values:
    return
  header = values[0]

  addr_idx = _find_col_index(header, ADDRESS_COL_CANDIDATES)
  dup_idx  = _find_col_index(header, DUP_COL_CANDIDATES)
  if addr_idx < 0 or dup_idx < 0:
    return

  groups: Dict[str, List[int]] = {}
  for r, row in enumerate(values[1:], start=2):
    addr = (row[addr_idx].strip() if addr_idx < len(row) else "")
    groups.setdefault(addr, []).append(r)

  dup_col = [header[dup_idx]] + [""] * (len(values) - 1)
  for addr, rows in groups.items():
    if not addr:
      continue
    if len(rows) >= 2:
      for k, r in enumerate(rows, start=1):
        dup_col[r - 1] = f"중복{k}"

  col_letter = _col_letter(dup_idx + 1)
  ws.update(f"{col_letter}1:{col_letter}{len(dup_col)}",
            [[v] for v in dup_col],
            value_input_option="USER_ENTERED")

def _load_existing_from_sheet(ws) -> Tuple[set, Dict[str, int]]:
  """- seen_urls: 이미 저장된 URL 집합 / - address_counts: 주소별 누계"""
  values = ws.get_all_values()
  if not values:
    return set(), {}
  header = values[0]

  url_idx  = _find_col_index(header, URL_COL_CANDIDATES)
  addr_idx = _find_col_index(header, ADDRESS_COL_CANDIDATES)

  seen_urls = set()
  address_counts: Dict[str, int] = {}
  for row in values[1:]:
    url = (row[url_idx].strip() if 0 <= url_idx < len(row) else "")
    addr = (row[addr_idx].strip() if 0 <= addr_idx < len(row) else "")
    if url:
      seen_urls.add(url)
    if addr:
      address_counts[addr] = address_counts.get(addr, 0) + 1
  return seen_urls, address_counts

def _append_rows(ws, rows: List[Dict[str, str]]):
  if not rows:
    return
  values = []
  for r in rows:
    values.append([
      r.get("no", ""),
      r.get("trust_name", ""),
      r.get("title", ""),
      r.get("post_date", ""),
      r.get("address", ""),
      r.get("city", ""),
      r.get("building", ""),
      r.get("sale_content", ""),
      r.get("purpose", ""),
      r.get("duplicate", ""),
      r.get("url", ""),
    ])
  ws.append_rows(values, value_input_option="USER_ENTERED")

# ────────── 상태 파일 ──────────
def _load_state() -> Dict:
  try:
    if os.path.exists(STATE_FILE):
      with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)
  except Exception:
    pass
  return {"last_done_page": None, "seen_urls": []}

def _save_state(state: Dict):
  try:
    os.makedirs("output", exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
      json.dump(state, f, ensure_ascii=False)
  except Exception:
    pass

# ────────── 보조 파서 ──────────
def _normalize_date(s: str) -> str:
  s = (s or "").strip()
  m = DATE_RE.search(s)
  if not m:
    return ""
  return m.group(0).replace(".", "-")

def _make_purpose(title: str) -> str:
  return "오피스텔" if "오피스텔" in (title or "") else ""

# ────────── 리스트 파서 (신영 전용) ──────────
def parse_list_page(html: str) -> List[Dict[str, str]]:
  """
  구조 예시:
  <tbody>
    <tr>
      <td class="idx">662</td>
      <td class="subject2"><a href="?pages=public&subs=sub01&m=view&idx=717&q=">제목</a></td>
      <td class="file"><button ...>진행중</button></td>
      <td class="file"><img ...></td>
      <td class="date">2025-09-15</td>
    </tr>
    ...
  </tbody>
  """
  soup = BeautifulSoup(html, "lxml")
  items: List[Dict[str, str]] = []

  for tr in soup.select("tbody > tr"):
    # 번호
    no_el = tr.select_one("td.idx")
    no = re.search(r"\d+", no_el.get_text(strip=True) if no_el else "")

    # 제목/링크
    a = tr.select_one("td.subject2 a[href]")
    title = a.get_text(" ", strip=True) if a else ""
    href = a.get("href", "") if a else ""
    url = urljoin(BASE, href) if href else ""

    # 게시일
    date_el = tr.select_one("td.date")
    post_date = _normalize_date(date_el.get_text(strip=True) if date_el else "")

    # 파생 필드
    address = extract_address(title) or ""
    city = extract_province_sgg(title, use_address_fallback=True) or ""
    building = extract_building_name(title) or ""
    sale_content = extract_sale_content(title) or ""
    purpose = _make_purpose(title)

    if title and url:
      items.append({
        "no": (no.group(0) if no else ""),
        "trust_name": TRUST_NAME,
        "title": title,
        "post_date": post_date,
        "url": url,
        "address": address,
        "city": city,
        "building": building,
        "sale_content": sale_content,
        "purpose": purpose,
      })

  return items

# ────────── 메인 ──────────
def main():
  setup_logging()
  ws = _open_sheet()

  # 1) 기존 시트의 중복열 정규화(2건 이상만 '중복1..N', 1건은 공란)
  _normalize_duplicate_numbering(ws)

  # 2) 시트/상태 기반으로 seen_urls / address_counts 복원
  seen_urls_sheet, address_counts = _load_existing_from_sheet(ws)
  state = _load_state()
  seen_urls = set(seen_urls_sheet) | set(state.get("seen_urls") or [])
  last_done = state.get("last_done_page")

  # 3) 재개 지점 계산
  start_page = START_PAGE
  if RESUME and last_done is not None:
    start_page = last_done + (1 if STEP > 0 else -1)

  logging.info("크롤 범위: %s → %s (step=%s)", start_page, END_PAGE, STEP)
  session = make_session()

  try:
    rng_end_inclusive = END_PAGE + (1 if STEP > 0 else -1)
    for page in range(start_page, rng_end_inclusive, STEP):
      params = {"pages": "public", "subs": "sub01", "pageno": page}
      try:
        r = session.get(urljoin(BASE, LIST_PATH), params=params, timeout=15)
        if r.status_code != 200:
          logging.warning("[LIST] page=%s status=%s", page, r.status_code)
          time.sleep(DELAY_SEC)
          continue
        if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
          r.encoding = r.apparent_encoding or "utf-8"
        html = r.text
      except requests.RequestException as e:
        logging.error("[LIST] page=%s error=%s", page, e)
        time.sleep(DELAY_SEC)
        continue

      items = parse_list_page(html)
      # 페이지 내 역순 처리(교보/대신/리츠피아와 동일)
      items = list(reversed(items))

      batch: List[Dict[str, str]] = []
      for it in items:
        u = it["url"]
        if u in seen_urls:
          continue

        addr = (it.get("address") or "").strip()
        duplicate = ""
        if addr:
          prev = address_counts.get(addr, 0)
          if prev >= 1:
            duplicate = f"중복{prev + 1}"  # 임시(첫 건은 공란)
          address_counts[addr] = prev + 1

        it["duplicate"] = duplicate
        batch.append(it)
        seen_urls.add(u)

      if batch:
        _append_rows(ws, batch)
        _normalize_duplicate_numbering(ws)
        logging.info("[LIST] page=%s appended rows=%d", page, len(batch))

      # 상태 저장
      state["last_done_page"] = page
      state["seen_urls"] = list(seen_urls)
      _save_state(state)

      time.sleep(DELAY_SEC)

  finally:
    logging.info("수집 종료")

if __name__ == "__main__":
  main()
