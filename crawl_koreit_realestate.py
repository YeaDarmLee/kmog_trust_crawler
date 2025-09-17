import time, subprocess, json, re
from typing import Dict, List, Tuple
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
from urllib.parse import urljoin

from address_only_regex import (
  extract_address,
  extract_province_sgg,
  extract_building_name,
  extract_sale_content,
)

def _purpose_officetel_flag(title: str) -> str:
  # 기존 스키마 유지: 오피스텔 여부만 '용도'에 표기
  return "오피스텔" if "오피스텔" in (title or "") else ""

# 이미 열린 크롬 세션 사용 (쿠키/로그인 공유)
subprocess.Popen(
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\\chromeCookie\\kmong_Rohmin_nol"'
)

# Selenium 옵션 설정
options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--ignore-certificate-errors')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

# ChromeDriver 실행
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# 사이트 진입
driver.get('https://kitrust.com/property/auction')
time.sleep(1)

items: List[Dict[str, str]] = []
TRUST_NAME = "한국투자부동산신탁"
wait1 = WebDriverWait(driver, 5)
wait2 = WebDriverWait(driver, 20)

# === XPATH/CSS 상수 ===
GRID_X        = "/html/body/main/div[2]/div/div/div/div[1]"
CARD_X        = GRID_X + "/div"
LOAD_MORE_X   = "/html/body/main/div[2]/div/div/div/div[2]/button"

# 카드 내부 제목(h3)
CARD_TITLE_REL_X = ".//h3"

# 모달
MODAL_ROOT   = "/html/body/div[6]/div[1]"
MODAL_CLOSE  = MODAL_ROOT + "/button"

TOTAL_COUNT_X = "/html/body/main/div[1]/section/div[3]/div/div/div/div[1]/div[2]/span"

# === 전체 count (숫자만 나옴) ===
total_count = int(wait1.until(EC.presence_of_element_located((By.XPATH, TOTAL_COUNT_X))).text.strip())
print("총 개수:", total_count)

processed = 0  # 처리한 카드 개수

def get_cards_count() -> int:
  return len(driver.find_elements(By.XPATH, CARD_X))

def get_card(i: int):
  # XPath 인덱스는 1-base, enumerate는 0-base
  return wait1.until(EC.element_to_be_clickable((By.XPATH, f"{CARD_X}[{i+1}]")))

def click_card_with_retry(i: int, retries: int = 2) -> bool:
  for attempt in range(retries + 1):
    try:
      el = get_card(i)
      driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
      time.sleep(0.1)
      el.click()
      return True
    except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException):
      if attempt == retries:
        return False
      time.sleep(0.35)

# 기존 read_modal_url_from_href() 전체 교체
def read_modal_url_from_href() -> str:
  """
  모달 내부의 '온비드 입찰 바로가기' 링크만 대기해서 반환.
  - 못 찾으면 TimeoutException 발생 (fallback 없음)
  """
  wait1.until(EC.visibility_of_element_located((By.XPATH, MODAL_ROOT)))
  # 온비드 고정 패턴: /op/cta/cltrdtl 포함
  onbid_css = 'a[href*="onbid.co.kr/op/cta/cltrdtl"]'
  link_el = WebDriverWait(driver, 3).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, onbid_css))
  )
  href = (link_el.get_attribute("href") or "").strip()
  if not href:
    # 온비드만 쓰기로 했으므로, href 없으면 에러로 처리
    raise TimeoutException("온비드 링크 href를 찾지 못했습니다.")
  return href

def close_modal():
  driver.find_element(By.XPATH, MODAL_CLOSE).click()
  wait1.until(EC.invisibility_of_element_located((By.XPATH, MODAL_ROOT)))

def process_range(start_idx: int):
  global processed
  total = get_cards_count()
  for i in range(start_idx, total):
    row_no = total_count - i
    try:
      # 카드 스크롤 & h3 제목 추출
      el = get_card(i)
      driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
      time.sleep(0.1)
      try:
        h3 = el.find_element(By.XPATH, CARD_TITLE_REL_X)
        card_title = (h3.text or "").strip()
      except Exception:
        card_title = ""

      # 카드 클릭 → 모달에서 href로 URL 추출(온비드 우선)
      if not click_card_with_retry(i):
        continue
      url = read_modal_url_from_href()

      # 파싱은 카드 제목 기준
      title = card_title
      address = extract_address(title) or ""
      city = extract_province_sgg(title, use_address_fallback=True) or ""
      building = extract_building_name(title) or ""
      sale_content = extract_sale_content(title) or ""
      purpose = _purpose_officetel_flag(title)

      print(f"[{row_no}] {title}")
      print(url)

      items.append({
        "no": row_no,
        "trust_name": TRUST_NAME,
        "title": title,
        "post_date": "",
        "url": url,
        "address": address,
        "city": city,
        "building": building,
        "sale_content": sale_content,
        "purpose": purpose,
      })

    except Exception as e:
      title = card_title
      address = extract_address(title) or ""
      city = extract_province_sgg(title, use_address_fallback=True) or ""
      building = extract_building_name(title) or ""
      sale_content = extract_sale_content(title) or ""
      purpose = _purpose_officetel_flag(title)
      
      print(f"[{row_no}] {title}")
      print("!!!! 에러발생 !!!!")
      items.append({
        "no": row_no,
        "trust_name": TRUST_NAME,
        "title": title,
        "post_date": "",
        "url": "해당 물건 정보를 찾을 수 없습니다",
        "address": address,
        "city": city,
        "building": building,
        "sale_content": sale_content,
        "purpose": purpose,
      })

    # 모달 닫기
    try:
      close_modal()
    except Exception:
      pass
    time.sleep(0.3)

  processed = total

def click_load_more_and_wait() -> bool:
  before = get_cards_count()
  try:
    btn = wait2.until(EC.element_to_be_clickable((By.XPATH, LOAD_MORE_X)))
  except TimeoutException:
    return False

  driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
  time.sleep(0.2)
  try:
    btn.click()
  except Exception:
    driver.execute_script("arguments[0].click();", btn)

  try:
    wait1.until(lambda d: get_cards_count() > before)
    return True
  except TimeoutException:
    return False

# === 실행 흐름 ===
wait1.until(EC.presence_of_element_located((By.XPATH, GRID_X)))
wait1.until(EC.presence_of_all_elements_located((By.XPATH, CARD_X)))

# 첫 페이지 처리
process_range(processed)

# 더보기 루프
while True:
  loaded = click_load_more_and_wait()
  if not loaded:
    print("끝 (더보기 버튼 없음 또는 추가 로드 안 됨)")
    break
  process_range(processed)
  time.sleep(0.6)

# 페이지 역순 (1부터 오름차순으로 보이게)
items = list(reversed(items))

# --- 디버그(선택): 배치 내 고유 URL/빈 URL 체크 ---
uniq = len({(it.get("url") or "").strip() for it in items if (it.get("url") or "").strip()})
empty = sum(1 for it in items if not (it.get("url") or "").strip())
print(f"DEBUG: 배치 내 고유 URL={uniq}, 빈 URL={empty}, 총={len(items)}")

# === ⬇️ Google Sheets 업로드 (URL 중복이어도 모두 업로드) ⬇️ ===
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID   = "1BEoi3Q6pOoUBUcEDgdy1YF03Ehc1hY02KfKz31GMt7E"
WORKSHEET_NAME   = "한국투자부동산_신탁"
SERVICE_KEY_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = [
  "번호","신탁사명","게시글 제목","게시일",
  "주소 (지번)","도시명","건물명","매각내용","용도",
  "중복여부","원문 링크(URL)"
]

def _open_sheet():
  creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=SCOPES)
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(SPREADSHEET_ID)
  try:
    ws = sh.worksheet(WORKSHEET_NAME)
  except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="2000", cols="20")
  vals = ws.get_all_values()
  if not vals:
    ws.append_row(HEADER)
  elif vals[0] != HEADER:
    pass
  return ws

def _find_col_index(header: list, name: str) -> int:
  try:
    return header.index(name)
  except ValueError:
    return -1

def _load_existing(ws):
  vals = ws.get_all_values()
  if not vals:
    return {}, {}
  header = vals[0]
  addr_idx = _find_col_index(header, "주소 (지번)")
  addr_counts = {}
  for row in vals[1:]:
    if 0 <= addr_idx < len(row):
      a = row[addr_idx].strip()
      if a:
        addr_counts[a] = addr_counts.get(a, 0) + 1
  return {}, addr_counts   # URL 중복 체크 안 함 (모두 업로드)

def _append_rows(ws, rows):
  if not rows:
    return
  ws.append_rows(rows, value_input_option="USER_ENTERED")

def _normalize_duplicate(ws):
  vals = ws.get_all_values()
  if not vals:
    return
  header = vals[0]
  try:
    dup_idx = header.index("중복여부")
    addr_idx = header.index("주소 (지번)")
  except ValueError:
    return

  # 주소별 그룹핑
  groups = {}
  for r, row in enumerate(vals[1:], start=2):
    addr = row[addr_idx].strip() if addr_idx < len(row) else ""
    groups.setdefault(addr, []).append(r)

  # 중복 라벨 재계산
  col = [header[dup_idx]] + [""] * (len(vals) - 1)
  for addr, rows in groups.items():
    if not addr:
      continue
    if len(rows) >= 2:
      for k, rr in enumerate(rows, start=1):
        col[rr - 1] = f"중복{k}"

  # 컬럼 갱신
  def _col_letter(n: int) -> str:
    s = ""
    while n:
      n, r = divmod(n - 1, 26)
      s = chr(r + 65) + s
    return s

  col_letter = _col_letter(dup_idx + 1)
  ws.update(
    [[v] for v in col],
    range_name=f"{col_letter}1:{col_letter}{len(col)}",
    value_input_option="USER_ENTERED"
  )

# ==== 업로드 실행 ====
ws = _open_sheet()
_, addr_counts = _load_existing(ws)

rows_to_append = []
for it in items:
  # URL 중복이어도 무조건 업로드
  url = (it.get("url") or "").strip()
  addr = (it.get("address") or "").strip()

  # 주소 기반 중복 라벨
  dup_label = ""
  if addr:
    prev = addr_counts.get(addr, 0)
    if prev >= 1:
      dup_label = f"중복{prev + 1}"
    addr_counts[addr] = prev + 1

  rows_to_append.append([
    it.get("no", ""),
    it.get("trust_name", ""),
    it.get("title", ""),
    it.get("post_date", ""),
    addr,
    it.get("city", ""),
    it.get("building", ""),
    it.get("sale_content", ""),
    it.get("purpose", ""),
    dup_label,
    url
  ])

_append_rows(ws, rows_to_append)
_normalize_duplicate(ws)
print(f"[OK] appended: {len(rows_to_append)} rows")
