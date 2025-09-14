# -*- coding: utf-8 -*-
# crawl_mghat.py (final)
# 무궁화신탁 매각 게시판 크롤링 → CSV 저장
# 추가 수집 필드:
# - address, city, building, sale_content (address_only_regex.py 사용)
# - purpose: title에 '오피스텔' 포함 시 '오피스텔'
# - duplicate: 같은 address가 기존/이번 배치에 이미 있으면 '중복'

import os
import re
import csv
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# ── 주소/도시/건물/매각내용 파생 함수 ──
from address_only_regex import (
  extract_address,
  extract_province_sgg,
  extract_building_name,
  extract_sale_content,
)

# 접속 베이스 후보 (SSL 이슈 대비 http 우선)
BASE_CANDIDATES = [
  "http://mghat.com",
  "http://www.mghat.com",
  "https://mghat.com",
  "https://www.mghat.com",
]
LIST_PATH = "/auction/disposal/list.do"
DETAIL_HREF_RE = re.compile(r"^/auction/disposal/\d+/show\.do(\?.*)?$")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

# === 설정 ===
START_PAGE = 249   # 과거 페이지
END_PAGE   = 1   # 최신 페이지
STEP     = -1  # 249 → 1
DELAY_SEC  = 1.0   # 예의상 지연

RESUME = True
STATE_FILE = "output/.mghat_state.json"

# 기존 CSV와 스키마 충돌을 피하려면 새 파일명 사용 권장
OUTPUT_CSV = "output/mghat_list_all_v2.csv"

# ─────────────────────────────────────────────────────────────
def setup_logging():
  os.makedirs("logs", exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  logfile = f"logs/mghat_list_{ts}.log"
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler()],
  )
  logging.info("로그 시작: %s", logfile)

def make_session() -> requests.Session:
  s = requests.Session()
  s.headers.update({
    "User-Agent": (
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
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

def _full_url(base: str, href: str) -> str:
  if href.startswith("http"):
    return href
  return base.rstrip("/") + href

def _try_fetch(session: requests.Session, base: str, page: int) -> Optional[str]:
  params = {"searchCount": "0", "field": "", "keyword": "", "type": "", "page": str(page)}
  url = base.rstrip("/") + LIST_PATH
  try:
    r = session.get(url, params=params, timeout=15)
    if r.status_code != 200:
      logging.warning("page=%s base=%s status=%s", page, base, r.status_code)
      return None
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
      r.encoding = r.apparent_encoding or "utf-8"
    return r.text
  except requests.exceptions.SSLError as e:
    logging.error("SSL 오류(base=%s, page=%s): %s", base, page, e)
    return None
  except requests.RequestException as e:
    logging.error("요청 오류(base=%s, page=%s): %s", base, page, e)
    return None

def fetch_list_page(session: requests.Session, page: int) -> Tuple[Optional[str], Optional[str]]:
  for base in BASE_CANDIDATES:
    html = _try_fetch(session, base, page)
    if html:
      return html, base
  return None, None

def _find_first_date_in_row(row) -> str:
  # 1) pc 뷰
  txt_cells = row.select("td.txt-gray")
  for td in txt_cells:
    t = td.get_text(" ", strip=True)
    m = DATE_RE.search(t)
    if m:
      return m.group(0)
  # 2) 모바일 보강
  mview = row.select_one(".m-date-view span")
  if mview:
    m = DATE_RE.search(mview.get_text(" ", strip=True))
    if m:
      return m.group(0)
  return ""

# ─────────────────────────────────────────────────────────────
# 파생 컬럼 생성
def _make_purpose(title: str) -> str:
  return "오피스텔" if "오피스텔" in (title or "") else ""

def _enrich_row(it: Dict[str, str]) -> Dict[str, str]:
  """
  title로부터 address/city/building/sale_content/purpose 파생 필드를 채워 넣는다.
  duplicate는 기존 CSV와 현재 배치의 주소 빈도 기반으로 쓰기 직전에 표기.
  """
  title = it.get("title", "") or ""
  try:
    addr = extract_address(title) or ""
  except Exception:
    addr = ""
  try:
    city = extract_province_sgg(title, use_address_fallback=True) or ""
  except Exception:
    city = ""
  try:
    building = extract_building_name(title) or ""
  except Exception:
    building = ""
  try:
    sale = extract_sale_content(title) or ""
  except Exception:
    sale = ""

  it["address"] = addr
  it["city"] = city
  it["building"] = building
  it["sale_content"] = sale
  it["purpose"] = _make_purpose(title)
  # it["duplicate"]는 쓰기 직전에 채움
  return it

def parse_list_items(html: str, base: str) -> List[Dict[str, str]]:
  soup = BeautifulSoup(html, "lxml")
  tbody = soup.select_one(".board-lst table tbody")
  items: List[Dict[str, str]] = []

  rows = tbody.select("tr") if tbody else []
  if not rows:
    # 구조변경 방어: 전체 a에서 추출
    links = soup.find_all("a", href=DETAIL_HREF_RE)
    for a in links:
      row = a.find_parent("tr") or a.find_parent("li") or a.parent
      title = a.get_text(strip=True)
      date_str = _find_first_date_in_row(row) if row else ""
      url = _full_url(base, a.get("href", "").strip())
      items.append(_enrich_row({
        "trust_name": "무궁화신탁",
        "title": title,
        "post_date": date_str,
        "url": url,
      }))
    return items

  for row in rows:
    a = row.select_one("td.tit a[href]")
    if not a:
      continue
    href = a.get("href", "").strip()
    if not DETAIL_HREF_RE.match(href):
      continue
    title = a.get_text(strip=True)
    date_str = _find_first_date_in_row(row)
    url = _full_url(base, href)
    items.append(_enrich_row({
      "trust_name": "무궁화신탁",
      "title": title,
      "post_date": date_str,
      "url": url,
    }))
  return items

# ─────────────────────────────────────────────────────────────
# CSV 유틸
def _csv_exists(path: str) -> bool:
  return os.path.exists(path) and os.path.getsize(path) > 0

def _open_csv_writer(path: str):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  file_exists = _csv_exists(path)
  f = open(path, "a", newline="", encoding="utf-8-sig")
  # ▼ 헤더 확장: purpose, duplicate 포함
  fieldnames = [
    "trust_name", "title", "post_date", "url",
    "address", "city", "building", "sale_content",
    "purpose", "duplicate",
  ]
  w = csv.DictWriter(f, fieldnames=fieldnames)
  if not file_exists:
    w.writeheader()
  return f, w

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

def _load_existing_addresses(path: str) -> Dict[str, int]:
  """
  기존 CSV의 address 컬럼 빈도 맵. RESUME나 재실행 시 중복 표기 일관성 유지.
  """
  counts: Dict[str, int] = {}
  if not _csv_exists(path):
    return counts
  try:
    with open(path, "r", encoding="utf-8-sig", newline="") as rf:
      rd = csv.DictReader(rf)
      for row in rd:
        addr = (row.get("address") or "").strip()
        if addr:
          counts[addr] = counts.get(addr, 0) + 1
  except Exception:
    pass
  return counts

# ─────────────────────────────────────────────────────────────
def main():
  setup_logging()
  session = make_session()

  state = _load_state()
  seen_urls = set(state.get("seen_urls") or [])
  last_done = state.get("last_done_page")

  start_page = START_PAGE
  if RESUME and last_done is not None:
    start_page = last_done + STEP

  logging.info("크롤 범위: %d → %d (step=%d)", start_page, END_PAGE, STEP)
  f, writer = _open_csv_writer(OUTPUT_CSV)

  # 기존 CSV의 address 빈도 로드 → duplicate 판정에 사용
  address_counts = _load_existing_addresses(OUTPUT_CSV)

  try:
    for page in range(start_page, END_PAGE - (1 if STEP < 0 else -1), STEP):
      logging.info("[LIST] 페이지 수집 시작 page=%s", page)
      html, base_used = fetch_list_page(session, page)
      if not html or not base_used:
        logging.warning("[LIST] page=%s 응답 없음 → 다음", page)
        time.sleep(DELAY_SEC)
        continue

      items = parse_list_items(html, base_used)

      # 중복 방지 후 CSV 누적 저장
      new_count = 0
      for it in items:
        url = it["url"]
        if url in seen_urls:
          continue

        # duplicate 판정: 동일 address가 과거/현재에 이미 있으면 '중복'
        addr = (it.get("address") or "").strip()
        duplicate = ""
        if addr:
          prev = address_counts.get(addr, 0)
          if prev >= 1:
            duplicate = "중복"
          address_counts[addr] = prev + 1  # 현재 건 반영
        it["duplicate"] = duplicate

        writer.writerow(it)
        seen_urls.add(url)
        new_count += 1

      logging.info("[LIST] page=%s 저장 rows(new)=%d / 누적 unique=%d",
             page, new_count, len(seen_urls))

      # 상태 저장(체크포인트)
      state["last_done_page"] = page
      state["seen_urls"] = list(seen_urls)
      _save_state(state)

      time.sleep(DELAY_SEC)
  finally:
    try:
      f.flush()
      f.close()
    except Exception:
      pass
    logging.info("CSV 닫기 및 종료")

if __name__ == "__main__":
  main()
