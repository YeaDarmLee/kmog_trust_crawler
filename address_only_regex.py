# -*- coding: utf-8 -*-
# address_only_regex.py (v12)
# - [공매]/[재공매]/[3차공매] 등 태그 무시
# - 세종특별자치시 특수 처리
# - 광역/도 약칭(서울/부산/인천/경기/충남/전북/제주 등) 대응
# - '시/군/구 단독 시작' + '광역 + 원시도시 + 동/읍/면' + '광역 + 동/읍/면' 대응
# - 지번 하이픈(ASCII/유니코드) 인식 보강
# - TOWN_RELAX를 '한글만' 허용(숫자 토큰을 동/리로 오인하지 않도록)
# - 건물명에서 행정구역 토큰(○○시/군/구/도, 인천광역시 등) 제외
# - 매각내용에서 태그/주소/건물명 제거 및 '외 N...' → 'N...' 정리

import re

# ─────────────────────────────────────────────────────────────
# 기본 토큰
PREFIX = r"(?:\d+\.\s*)?(?:\[[^\]]+\]\s*)*"

# 세종 전용 토큰 (광역명)
PROV_SEJONG = r"(?:세종특별자치시|세종시|세종)"

# 광역/도 목록 (세종 제외) — 광역시/도 약칭 포함
REG1_NO_SEJONG = (
  r"(?:" 
  r"서울특별시|서울시|서울|"
  r"부산광역시|부산시|부산|"
  r"대구광역시|대구시|대구|"
  r"인천광역시|인천시|인천|"
  r"광주광역시|광주시|광주|"
  r"대전광역시|대전시|대전|"
  r"울산광역시|울산시|울산|"
  r"경기도|경기|"
  r"강원특별자치도|강원도|강원|"
  r"충청북도|충북|"
  r"충청남도|충남|"
  r"전북특별자치도|전라북도|전북|"
  r"전라남도|전남|"
  r"경상북도|경북|"
  r"경상남도|경남|"
  r"제주특별자치도|제주도|제주"
  r")"
)

# 전체 광역/도 (세종 포함)
REG1 = rf"(?:{PROV_SEJONG}|{REG1_NO_SEJONG})"

# 시/군/구 — 뒤에 공백/끝 경계 요구
CITY_GUN_GU = r"(?:[가-힣]+(?:시|군|구))(?=\s|$)"

# 광역 뒤에 붙는 '원시 도시명'(접미사 없음: 파주/포천/양양/홍성 등)
RAW_CITY = r"(?:[가-힣]{2,})"

# 읍/면/동/가/리
TOWN_STRICT = r"(?:[가-힣0-9]+(?:읍|면|동|가|리))"
# ⚠ 숫자만(예: '94')이 동/리로 오인되지 않도록 한글만 허용
TOWN_RELAX  = r"(?:[가-힣]{2,})"
TOWN = rf"(?:{TOWN_STRICT}|{TOWN_RELAX})"

# 도로명
ROAD = r"(?:[가-힣0-9]+(?:로|길|대로|번길))"

# 하이픈(ASCII/유니코드)
HYP = r"[-–—−﹣－]"

# 지번/산 (가-660-2, 94-3 등)
LOT = rf"(?:산\s*)?(?:[가-힣]{HYP}\d+|\d+)(?:{HYP}\d+)?(?:\s*(?:일원|번지))?"
LOT_LIST = rf"{LOT}(?:\s*,\s*(?:[가-힣]{HYP}\d+|\d+)(?:{HYP}\d+)?)*"

# 도시계획/구획 명칭 — 지구/구역까지 주소에 포함
PLAN_DIST = r"(?:[가-힣0-9]+(?:지구|구역))"

# ─────────────────────────────────────────────────────────────
# 1) 전체 주소만 추출 — 세종/일반/시군구 단독/보강 패턴
PATTERN = rf"""
^(?P<addr>
  {PREFIX}
  (?:
  # ◇ 세종 특수: 시/군/구 없이 읍/면/동부터 가능
  (?P<prov_sj>{PROV_SEJONG})
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 일반: 광역/도 + (시/군/구 1개 이상)
  (?P<prov_etc>{REG1_NO_SEJONG})
  (?:\s+{CITY_GUN_GU})+
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 보조1: 시/군/구 단독 시작 (예: '전주시 완산구 고사동 408-3', '서초구 서초동 1445')
  (?P<sgg_only>{CITY_GUN_GU})
  (?:\s+{CITY_GUN_GU})*
  (?:\s+{TOWN}){{0,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 보강2: 광역/도 + '원시 도시명' + 동/읍/면 (예: '경기 파주 야당동', '충남 홍성 오관리')
  (?P<prov_raw>{REG1_NO_SEJONG})
  \s+ (?P<rawcity>{RAW_CITY})
  (?:\s+{TOWN}){{1,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?

  |
  # ◇ 보강3: 광역/도 + 동/읍/면 직결 (예: '인천 만수동', '제주 한림읍')
  (?P<prov_town>{REG1_NO_SEJONG})
  (?:\s+{TOWN}){{1,3}}
  (?:\s+{PLAN_DIST})?
  (?:\s+{ROAD})?
  (?:\s*{LOT_LIST})?
  )
)
"""
RX_ADDR = re.compile(PATTERN, re.X)

def extract_address(title: str) -> str:
  m = RX_ADDR.search(title or "")
  if not m:
    return ""
  # 대괄호 태그 등 PREFIX 제거
  return re.sub(rf"^{PREFIX}", "", m.group("addr")).strip()

# ─────────────────────────────────────────────────────────────
# 2) 시군구(도시명)
PATTERN_SGG = rf"""
^{PREFIX}
(?P<prov>{REG1_NO_SEJONG})
(?:\s+(?P<sgg1>{CITY_GUN_GU}))
(?:\s+(?P<sgg2>{CITY_GUN_GU}))?
"""
RX_SGG = re.compile(PATTERN_SGG, re.X)

# 세종 전용
RX_SGJ = re.compile(rf"^{PREFIX}(?P<prov>{PROV_SEJONG})\b")

# 시/군/구 단독 시작
PATTERN_SGG_ONLY = rf"""
^{PREFIX}
(?P<sgg1>{CITY_GUN_GU})
(?:\s+(?P<sgg2>{CITY_GUN_GU}))?
"""
RX_SGG_ONLY = re.compile(PATTERN_SGG_ONLY, re.X)

# 광역/도 + '원시 도시명' + 동/읍/면
PATTERN_PROV_RAWCITY = rf"""
^{PREFIX}
(?P<prov_raw2>{REG1_NO_SEJONG})
\s+(?P<rawcity2>{RAW_CITY})
\s+{TOWN}
"""
RX_PROV_RAWCITY = re.compile(PATTERN_PROV_RAWCITY, re.X)

# 광역/도 + 동/읍/면
PATTERN_PROV_TOWN = rf"""
^{PREFIX}
(?P<prov_town2>{REG1_NO_SEJONG})
\s+{TOWN}
"""
RX_PROV_TOWN = re.compile(PATTERN_PROV_TOWN, re.X)

# 표준화 매핑 — 약칭 → 정식 명칭
_PROV_STD = {
  "서울": "서울특별시", "서울시": "서울특별시",
  "부산": "부산광역시", "부산시": "부산광역시",
  "대구": "대구광역시", "대구시": "대구광역시",
  "인천": "인천광역시", "인천시": "인천광역시",
  "광주": "광주광역시", "광주시": "광주광역시",
  "대전": "대전광역시", "대전시": "대전광역시",
  "울산": "울산광역시", "울산시": "울산광역시",
  "경기": "경기도",
  "강원": "강원특별자치도", "강원도": "강원특별자치도",
  "충북": "충청북도",
  "충남": "충청남도",
  "전라북도": "전북특별자치도", "전북": "전북특별자치도",
  "전남": "전라남도",
  "경북": "경상북도",
  "경남": "경상남도",
  "제주": "제주특별자치도", "제주도": "제주특별자치도",
  "세종": "세종특별자치시", "세종시": "세종특별자치시",
}

def _normalize_province(p: str) -> str:
  return _PROV_STD.get(p, p)

def extract_province_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  s = (title_or_address or "").strip()
  s = re.sub(rf"^{PREFIX}", "", s)

  # 1) 정규 '광역/도 + 시군구'
  m = RX_SGG.search(s)
  if m:
    prov = _normalize_province(m.group("prov") or "")
    sgg1 = m.group("sgg1") or ""
    sgg2 = m.group("sgg2") or ""
    return f"{prov} {sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

  # 2) 세종 특수
  m2 = RX_SGJ.search(s)
  if not m2 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m = RX_SGG.search(addr)
      if m:
        prov = _normalize_province(m.group("prov") or "")
        sgg1 = m.group("sgg1") or ""
        sgg2 = m.group("sgg2") or ""
        return f"{prov} {sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()
      m2 = RX_SGJ.search(addr)
  if m2:
    return _normalize_province(m2.group("prov"))

  # 3) 시/군/구 단독 시작
  m3 = RX_SGG_ONLY.search(s)
  if not m3 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m3 = RX_SGG_ONLY.search(addr)
  if m3:
    sgg1 = (m3.group("sgg1") or "").strip()
    sgg2 = (m3.group("sgg2") or "").strip()
    return f"{sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

  # 4) 광역/도 + 원시도시 + 동/읍/면
  m4 = RX_PROV_RAWCITY.search(s)
  if not m4 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m4 = RX_PROV_RAWCITY.search(addr)
  if m4:
    prov = _normalize_province(m4.group("prov_raw2") or "")
    rawc = (m4.group("rawcity2") or "").strip()
    return f"{prov} {rawc}".strip()

  # 5) 광역/도 + 동/읍/면 → 광역/도만
  m5 = RX_PROV_TOWN.search(s)
  if not m5 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m5 = RX_PROV_TOWN.search(addr)
  if m5:
    return _normalize_province(m5.group("prov_town2") or "")

  return ""

def extract_city_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  s = (title_or_address or "").strip()
  s = re.sub(rf"^{PREFIX}", "", s)

  m = RX_SGG.search(s)
  if m:
    sgg1 = m.group("sgg1") or ""
    sgg2 = m.group("sgg2") or ""
    return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

  m2 = RX_SGJ.search(s)
  if not m2 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m = RX_SGG.search(addr)
      if m:
        sgg1 = m.group("sgg1") or ""
        sgg2 = m.group("sgg2") or ""
        return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()
      m2 = RX_SGJ.search(addr)
  if m2:
    return ""

  m3 = RX_SGG_ONLY.search(s)
  if not m3 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m3 = RX_SGG_ONLY.search(addr)
  if m3:
    sgg1 = (m3.group("sgg1") or "").strip()
    sgg2 = (m3.group("sgg2") or "").strip()
    return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

  m4 = RX_PROV_RAWCITY.search(s)
  if not m4 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m4 = RX_PROV_RAWCITY.search(addr)
  if m4:
    return (m4.group("rawcity2") or "").strip()

  m5 = RX_PROV_TOWN.search(s)
  if not m5 and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m5 = RX_PROV_TOWN.search(addr)
  if m5:
    return ""

  return ""

# ─────────────────────────────────────────────────────────────
# 3) 건물명 추출 (행정구역 토큰 제외)
_BUILDING_STOP = re.compile(
  r"(?:일괄매각|개별매각|매각\s*공고|매각공고|재공매|재매각|후\s*개별수의계약\s*공고|"
  r"공매\s*공고|공매공고|입찰\s*공고|입찰공고|\b공고\b)"
)
_UNIT_TOKENS = re.compile(
  r"(?:제?[0-9A-Za-z\-]+동|제?[0-9A-Za-z\-]+층|제?[0-9A-Za-z\-]+호|[0-9]+동|[0-9]+층|[0-9]+호)"
)
_BRACKET = re.compile(r"[\[\(].*?[\]\)]")
_SEPARATORS = re.compile(r"[,\u00B7·/]|(?:\s+외\s+)")

_BUILDING_SUFFIX = (
  r"(?:타워|팰리스|캐슬|캐슬플러스|팰리움|밸리|스퀘어|시티|힐스|힐스테이트|"
  r"아이파크|자이|더힐|프라자|프라임|스카이|에버빌|해링턴타워|해링턴|아파트|"
  r"오피스텔|연립주택|주건축물|몰|빌라|빌리지|스포츠몰|블록|블럭|롯트|로트|생활형숙박시설)"
)
_BUILDING_CORE = r"[가-힣A-Za-z0-9\-·]+"
_BUILDING_CANDIDATE = re.compile(rf"({_BUILDING_CORE}(?:{_BUILDING_SUFFIX})?)")

_EXCLUDE_BUILDING = re.compile(
  rf"""^
    (?:외\s*\d+\s*)?
    (?:
      (?:[가-힣]{HYP}\d+|\d+)(?:{HYP}\d+)?(?:번지)?
      |
      \d+\s*개(?:\s*(?:호|호실|세대|필지))?
      |
      (?:호|호실|세대|필지)$
    )
  """, re.X
)

_ADMIN_SUFFIX = re.compile(r"(?:광역시|특별시|특별자치시|특별자치도|도|시|군|구)$")
_ADMIN_FULL = re.compile(
  r"^(?:서울특별시|서울시|서울|부산광역시|부산시|부산|대구광역시|대구시|대구|"
  r"인천광역시|인천시|인천|광주광역시|광주시|광주|대전광역시|대전시|대전|"
  r"울산광역시|울산시|울산|경기도|경기|강원특별자치도|강원도|강원|충청북도|충북|"
  r"충청남도|충남|전북특별자치도|전라북도|전북|전라남도|전남|경상북도|경북|"
  r"경상남도|경남|제주특별자치도|제주도|제주|세종특별자치시|세종시|세종)$"
)

# 공고/공매류 단어는 건물명 후보에서 제외
_FORBID_BUILDING = re.compile(
  r"^(?:공매공고|공매|입찰공고|입찰|매각공고|매각|공고|연기공고|연기)$"
)

def extract_building_name(title: str) -> str:
  s = (title or "").strip()
  s = re.sub(rf"^{PREFIX}", "", s)  # 태그 제거

  # 주소 제거: 문장 내 첫 발생 지점을 기준으로 tail 산출
  addr = extract_address(s)
  if addr:
    i = s.find(addr)
    tail = s[i + len(addr):].strip() if i >= 0 else s
  else:
    tail = s

  # 괄호/동·층·호/공고 키워드 제거
  tail = _BRACKET.sub(" ", tail)
  tail = _UNIT_TOKENS.sub(" ", tail)
  mstop = _BUILDING_STOP.search(tail)
  if mstop:
    tail = tail[:mstop.start()].strip()

  parts = [p.strip() for p in _SEPARATORS.split(tail) if p.strip()]
  best = ""

  def _is_admin_token(tok: str) -> bool:
    return bool(_ADMIN_FULL.match(tok) or _ADMIN_SUFFIX.search(tok))

  def score(x: str) -> tuple:
    suf = x.endswith((
      "타워","팰리스","캐슬","밸리","스퀘어","시티","힐스","아이파크","자이","더힐",
      "프라자","프라임","스카이","에버빌","해링턴타워","해링턴","아파트","오피스텔",
      "빌라","빌리지","몰","스포츠몰","블록","블럭","롯트","로트","생활형숙박시설"
    ))
    return (1 if suf else 0, len(x))

  for p in parts:
    for mm in _BUILDING_CANDIDATE.finditer(p):
      token = mm.group(1).strip("-·").strip()
      if len(token) < 2:
        continue
      if _EXCLUDE_BUILDING.match(token):
        continue
      if _is_admin_token(token):
        continue
      if _FORBID_BUILDING.match(token):  # ← 이 줄 추가!
        continue
      if not best or score(token) > score(best):
        best = token

  return best

# ─────────────────────────────────────────────────────────────
# 4) 매각내용: 주소/도시명/건물명 제거 후 정리
_SALE_KEYWORDS = re.compile(r"(?:일괄매각|개별매각|매각\s*공고|매각공고|재공매|재매각)")
def extract_sale_content(title: str) -> str:
  s = (title or "").strip()
  s = re.sub(rf"^{PREFIX}", "", s)  # 태그 제거

  # 1) 주소 제거 (첫 발생 위치 기준)
  addr = extract_address(s)
  if addr:
    i = s.find(addr)
    if i >= 0:
      s = (s[:i] + s[i + len(addr):]).strip()

  # 2) 도시명(광역/도 + 시군구 또는 시/군/구 시퀀스) 제거(앞부분에 남았을 경우)
  prov_sgg = extract_province_sgg(title, use_address_fallback=True)
  if prov_sgg:
    s = s.replace(prov_sgg, "", 1).strip()

  # 3) 건물명 제거
  bld = extract_building_name(title)
  if bld:
    s = s.replace(bld, "", 1).strip()

  # 4) 선두 '외 N...' → 'N...' 로 정리
  s = re.sub(r"^외\s*(\d+)\s*", r"\1", s)

  # 5) 공백/구두점 정리
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"\s*([,\u00B7·/])\s*", r"\1", s)
  s = re.sub(r"^[,·/]+", "", s)
  s = re.sub(r"[,·/]+$", "", s)
  return s.strip()
