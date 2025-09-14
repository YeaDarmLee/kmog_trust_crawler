# mh_disposal_list_crawl.py
# 무궁화신탁 공매/매각 게시판 전체 크롤링 → 단일 CSV 누적 저장
# - 249(과거) → 1(최신) 역순
# - 필드: trust_name, title, post_date, url, address, building, city, use, is_duplicate
# - 제목 파싱: address_title_parser.KoreanAuctionTitleParser 사용
# - URL 중복 스킵 / 제목 중복 표기 / Resume / HTTP→HTTPS 폴백

import os, re, time, csv, logging, json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# ====== 제목 파서 연결 (같은 디렉토리의 address_title_parser.py) ======
try:
  from address_title_parser import KoreanAuctionTitleParser
except Exception:
  import importlib.util
  _p = os.path.join(os.path.dirname(__file__), "address_title_parser.py")
  _spec = importlib.util.spec_from_file_location("address_title_parser", _p)
  if not _spec or not _spec.loader:
    raise
  _mod = importlib.util.module_from_spec(_spec)
  _spec.loader.exec_module(_mod)
  KoreanAuctionTitleParser = _mod.KoreanAuctionTitleParser

parser = KoreanAuctionTitleParser()
# 필요시 옵션 조정 예시:
# parser.fill_land_when_building_empty = True
# parser.multi_building_mode = "all"  # 또는 "first"

# 접속 베이스 후보 (SSL 회피를 위해 http 우선)
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
# START_PAGE = 249   # 과거
START_PAGE = 10   # 과거
END_PAGE   = 1     # 최신
STEP       = -1    # 249 → 1
DELAY_SEC  = 1.0   # politeness

RESUME = True
STATE_FILE = "output/.mghat_state.json"
OUTPUT_CSV = "output/mghat_list_all.csv"

def setup_logging():
  os.makedirs("logs", exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d_%H%M%S")
  logfile = f"logs/mghat_list_{ts}.log"
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(logfile, encoding="utf-8"), logging.StreamHandler()]
  )
  logging.info("로그 시작: %s", logfile)

def make_session() -> requests.Session:
  s = requests.Session()
  s.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/127.0.0.0 Safari/537.36",
    "Accept-Language": "ko,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
  })
  retries = Retry(
    total=3,
    backoff_factor=0.6,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False
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
  # 1) td.txt-gray 중 YYYY-MM-DD 매칭되는 첫 번째 사용
  txt_cells = row.select("td.txt-gray")
  for td in txt_cells:
    t = td.get_text(" ", strip=True)
    m = DATE_RE.search(t)
    if m:
      return m.group(0)
  # 2) 모바일용 div.m-date-view > span:first
  mview = row.select_one(".m-date-view span")
  if mview:
    m = DATE_RE.search(mview.get_text(" ", strip=True))
    if m:
      return m.group(0)
  return ""

def _norm_title(s: str) -> str:
  # 제목 중복 체크용 정규화: 공백/특수문자 축약 + 소문자
  s = re.sub(r"\s+", " ", s).strip().lower()
  s = re.sub(r"[^\w가-힣]+", "", s)
  return s

def _parse_title_fields(title: str) -> Tuple[str, str, str, str]:
  """
  address_title_parser 기반으로 address/building/city/use 추출
  - parser.parse(title) → List[dict]; 첫 결과 사용
  """
  try:
    rows = parser.parse(title) or []
    if not rows:
      return "", "", "", ""
    r0 = rows[0] or {}
    address = (r0.get("address") or "").strip()
    building = (r0.get("building") or "").strip()
    city = (r0.get("city") or "").strip()
    use = (r0.get("use") or "").strip()
    return address, building, city, use
  except Exception as e:
    logging.warning("제목 파싱 실패: %s | err=%s", title, e)
    return "", "", "", ""

def parse_list_items(html: str, base: str) -> List[Dict[str, str]]:
  soup = BeautifulSoup(html, "lxml")
  tbody = soup.select_one(".board-lst table tbody")
  items: List[Dict[str, str]] = []

  rows = tbody.select("tr") if tbody else []
  if not rows:
    # 구조 변경 대비: 전체 a에서 보강
    links = soup.find_all("a", href=DETAIL_HREF_RE)
    for a in links:
      row = a.find_parent("tr") or a.find_parent("li") or a.parent
      title = a.get_text(strip=True)
      date_str = _find_first_date_in_row(row) if row else ""
      url = _full_url(base, a.get("href", "").strip())
      address, building, city, use = _parse_title_fields(title)
      items.append({
        "trust_name": "무궁화신탁",
        "title": title,
        "post_date": date_str,
        "url": url,
        "address": address,
        "building": building,
        "city": city,
        "use": use,
      })
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
    address, building, city, use = _parse_title_fields(title)
    items.append({
      "trust_name": "무궁화신탁",
      "title": title,
      "post_date": date_str,
      "url": url,
      "address": address,
      "building": building,
      "city": city,
      "use": use,
    })
  return items

# ---------- 단일 CSV 누적 저장 + 상태 관리 ----------
FIELDNAMES = [
  "trust_name", "title", "post_date", "url",
  "address", "building", "city", "use",
  "is_duplicate"  # 제목 중복 여부(Y/N)
]

def _csv_exists(path: str) -> bool:
  return os.path.exists(path) and os.path.getsize(path) > 0

def _open_csv_writer(path: str):
  os.makedirs(os.path.dirname(path), exist_ok=True)
  file_exists = _csv_exists(path)
  f = open(path, "a", newline="", encoding="utf-8-sig")
  w = csv.DictWriter(f, fieldnames=FIELDNAMES)
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
  return {"last_done_page": None, "seen_urls": [], "seen_titles": []}

def _save_state(state: Dict):
  try:
    os.makedirs("output", exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
      json.dump(state, f, ensure_ascii=False)
  except Exception:
    pass
# -------------------------------------------

def main():
  setup_logging()
  session = make_session()

  state = _load_state()
  seen_urls = set(state.get("seen_urls") or [])
  seen_titles = set(state.get("seen_titles") or [])
  last_done = state.get("last_done_page")

  start_page = START_PAGE
  if RESUME and last_done is not None:
    start_page = last_done + STEP

  logging.info("크롤 범위: %d → %d (step=%d)", start_page, END_PAGE, STEP)
  f, writer = _open_csv_writer(OUTPUT_CSV)

  try:
    for page in range(start_page, END_PAGE - (1 if STEP < 0 else -1), STEP):
      logging.info("[LIST] 페이지 수집 시작 page=%s", page)
      html, base_used = fetch_list_page(session, page)
      if not html or not base_used:
        logging.warning("[LIST] page=%s 응답 없음 -> 다음", page)
        time.sleep(DELAY_SEC)
        continue

      logging.info("  사용 base: %s", base_used)

      items = parse_list_items(html, base_used)

      # 상위 3개 샘플 로그
      for i, it in enumerate(items[:3], start=1):
        logging.info("  #%d  [%s] %s | %s", i, it.get("post_date") or "-", it["title"], it["url"])

      # 쓰기: URL 완전중복은 스킵, 제목 중복은 표기만
      new_count = 0
      for it in items:
        url = it["url"]
        norm_title = _norm_title(it["title"])
        is_dup = "Y" if norm_title in seen_titles else "N"

        if url in seen_urls:
          continue

        row = {
          **it,
          "is_duplicate": is_dup
        }
        writer.writerow(row)
        new_count += 1

        seen_urls.add(url)
        seen_titles.add(norm_title)

      logging.info("[LIST] page=%s 저장 rows(new)=%d / 누적 unique_urls=%d / unique_titles=%d",
                   page, new_count, len(seen_urls), len(seen_titles))

      state["last_done_page"] = page
      state["seen_urls"] = list(seen_urls)
      state["seen_titles"] = list(seen_titles)
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
