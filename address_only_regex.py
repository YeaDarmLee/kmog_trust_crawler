# -*- coding: utf-8 -*-
# address_only_regex.py (v5) — 주소/시군구 동시 지원

import re

PREFIX = r"(?:\d+\.\s*)?"
REG1 = r"(?:서울특별시|서울시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시|세종시|경기도|강원특별자치도|강원도|충청북도|충북|충청남도|충남|전라북도|전북|전라남도|전남|경상북도|경북|경상남도|경남|제주특별자치도|제주도)"
CITY_GUN_GU = r"(?:[가-힣]+(?:시|군|구))"
# 읍/면/동/가/리 토큰 연속 0~3회 허용 (예: '한경면 저지리', '태안읍 남문리', '항동7가')
TOWN = r"(?:[가-힣0-9]+(?:읍|면|동|가|리))"
ROAD = r"(?:[가-힣0-9]+(?:로|길|대로|번길))"
LOT = r"(?:산\s*)?\d+(?:-\d+)?(?:\s*(?:일원|번지))?"
LOT_LIST = rf"{LOT}(?:\s*,\s*\d+(?:-\d+)?)*"

# ─────────────────────────────────────────────────────────────
# 1) 전체 주소만 추출 (검증 완료된 패턴)
PATTERN = rf"""
^(?P<addr>
  {PREFIX}
  {REG1}
  (?:\s+{CITY_GUN_GU})+           # 시/군/구: 1개 이상 (예: '천안시 서북구')
  (?:\s+{TOWN}){{0,3}}            # 읍/면/동/리: 최대 3토큰까지 허용
  (?:\s+{ROAD})?                  # 도로명 (선택)
  (?:\s*{LOT_LIST})?              # 지번/산/번지/일원 및 콤마 나열 (선택)
)
"""
RX_ADDR = re.compile(PATTERN, re.X)

def extract_address(title: str) -> str:
  """
  제목에서 '행정구역~지번'까지의 '주소만' 추출.
  - 건물명/동·층·호/공고 키워드는 포함하지 않음.
  """
  m = RX_ADDR.search(title or "")
  if not m:
    return ""
  return re.sub(r"^\d+\.\s*", "", m.group("addr")).strip()

# ─────────────────────────────────────────────────────────────
# 2) 시군구(도시명) 전용: "광역/도 + 시군구" 형태로 추출
#    예: "충청남도 천안시 서북구 백석동..." -> "충청남도 천안시 서북구"
#        "인천광역시 미추홀구 주안동..."   -> "인천광역시 미추홀구"
#        "경기도 양평군 서종면..."          -> "경기도 양평군"
#        "서울특별시 종로구 ..."           -> "서울특별시 종로구"

# 약칭을 표준으로 치환(출력 일관성용, 필요 시 수정)
_PROV_STD = {
  "서울시": "서울특별시",
  "강원도": "강원특별자치도",  # 원하면 이 매핑은 제거 가능
  "충북": "충청북도",
  "충남": "충청남도",
  "전북": "전라북도",
  "전남": "전라남도",
  "경북": "경상북도",
  "경남": "경상남도",
  "제주도": "제주특별자치도",
}

PATTERN_SGG = rf"""
^{PREFIX}
(?P<prov>{REG1})                # 광역/도
(?:\s+(?P<sgg1>{CITY_GUN_GU}))  # 첫번째 시/군/구 (필수)
(?:\s+(?P<sgg2>{CITY_GUN_GU}))? # 두번째 시/군/구 (선택)
"""
RX_SGG = re.compile(PATTERN_SGG, re.X)

def _normalize_province(p: str) -> str:
  return _PROV_STD.get(p, p)

def extract_province_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  """
  입력: 원본 제목 또는 주소 일부
  출력: '광역/도 + 시군구'
       예) '충청남도 천안시 서북구', '인천광역시 미추홀구', '경기도 양평군', '서울특별시 종로구'
  동작:
  - 번호 접두어('1. ') 제거
  - 약칭(서울시/경남 등) 표준 명칭으로 치환
  - use_address_fallback=True이면, 원문에서 매칭 실패 시 extract_address()로 주소를 뽑아 재시도
  """
  s = (title_or_address or "").strip()
  s = re.sub(r"^\d+\.\s*", "", s)

  m = RX_SGG.search(s)
  if not m and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m = RX_SGG.search(addr)

  if not m:
    return ""

  prov = _normalize_province(m.group("prov") or "")
  sgg1 = m.group("sgg1") or ""
  sgg2 = m.group("sgg2") or ""
  return f"{prov} {sgg1}{(' ' + sgg2) if sgg2 else ''}".strip()

def extract_city_sgg(title_or_address: str, *, use_address_fallback: bool = True) -> str:
  """
  입력: 원본 제목 또는 주소 일부
  출력: '시군구'만 (1~2토큰)
       예) '천안시 서북구', '미추홀구', '양평군', '종로구'
  """
  s = (title_or_address or "").strip()
  s = re.sub(r"^\d+\.\s*", "", s)

  m = RX_SGG.search(s)
  if not m and use_address_fallback:
    addr = extract_address(s)
    if addr:
      m = RX_SGG.search(addr)

  if not m:
    return ""

  sgg1 = m.group("sgg1") or ""
  sgg2 = m.group("sgg2") or ""
  return (sgg1 + (" " + sgg2 if sgg2 else "")).strip()

# ─────────────────────────────────────────────────────────────
# 3) 건물명 추출: 동/층/호 제외, 주소/공고 키워드 제거 후 남는 고유명사 추출
_BUILDING_STOP = re.compile(
  r"(?:일괄매각|개별매각|매각\s*공고|매각공고|재공매|재매각|후\s*개별수의계약\s*공고)"
)
_UNIT_TOKENS = re.compile(
  r"(?:제?[0-9A-Za-z\-]+동|제?[0-9A-Za-z\-]+층|제?[0-9A-Za-z\-]+호|[0-9]+동|[0-9]+층|[0-9]+호)"
)
_BRACKET = re.compile(r"[\[\(].*?[\]\)]")
_SEPARATORS = re.compile(r"[,\u00B7·/]|(?:\s+외\s+)")

# 건물명 접미사 힌트(없어도 동작, 있으면 정확도 ↑)
_BUILDING_SUFFIX = r"(?:타워|팰리스|캐슬|캐슬플러스|팰리움|밸리|스퀘어|시티|힐스|힐스테이트|아이파크|자이|더힐|프라자|프라임|스카이|에버빌|해링턴타워|해링턴|아파트|오피스텔|연립주택|주건축물|몰|빌라|빌리지|스포츠몰)"
# 건물명 본체(국문/영문/숫자/하이픈/중점)
_BUILDING_CORE = r"[가-힣A-Za-z0-9\-·]+"

_BUILDING_CANDIDATE = re.compile(
  rf"({_BUILDING_CORE}(?:{_BUILDING_SUFFIX})?)"
)

_EXCLUDE_BUILDING = re.compile(
  r"""^
      (?:외\s*\d+\s*)?             # '외1', '외 3' 등 접두
      (?:\d+\s*)?(?:개\s*)?        # '32개' 등
      (?:필지|호|호실|세대)\s*$     # 단위
  """, re.X
)

def extract_building_name(title: str) -> str:
  """
  주소/동·층·호/공고 키워드를 제거하고 남는 구간에서
  동호수 제외 '건물명' 1개를 반환.
  - '3필지', '32개 호실' 등 수량/단위 표현은 건물명에서 제외.
  """
  s = (title or "").strip()

  # 1) 주소 제거 후 남은 텍스트
  addr = extract_address(s)
  tail = s[len(addr):].strip() if addr and s.startswith(addr) else s

  # 2) 괄호/동·층·호/공고 키워드 제거
  tail = _BRACKET.sub(" ", tail)
  tail = _UNIT_TOKENS.sub(" ", tail)
  mstop = _BUILDING_STOP.search(tail)
  if mstop:
    tail = tail[:mstop.start()].strip()

  # 3) 구분자 분해 → 건물명 후보 스캔
  parts = [p.strip() for p in _SEPARATORS.split(tail) if p.strip()]
  best = ""

  def score(x: str) -> tuple:
    suf = x.endswith(("타워","팰리스","캐슬","밸리","스퀘어","시티","힐스","아이파크","자이","더힐","프라자","프라임","스카이","에버빌","해링턴타워","해링턴","아파트","오피스텔","빌라","빌리지","몰","스포츠몰"))
    return (1 if suf else 0, len(x))

  for p in parts:
    cands = []
    for mm in _BUILDING_CANDIDATE.finditer(p):
      token = mm.group(1).strip("-·").strip()
      # ① 최소 길이 ② 수량/단위 토큰 제외
      if len(token) >= 3 and not _EXCLUDE_BUILDING.match(token):
        cands.append(token)
    if cands:
      pick = sorted(cands, key=score, reverse=True)[0]
      if score(pick) > score(best):
        best = pick

  return best

# ─────────────────────────────────────────────────────────────
# 4) 매각내용(요약) 추출: 수량+대상+매각종류(일괄/개별/공고/재공매 등)
_SALE_PATTERNS = [
  # 110호 외 8개호실 개별매각 / 101호 외 1개호실 개별매각
  r"\b\d+\s*호\s*외\s*\d+\s*(?:개)?호실?\s*(?:일괄매각|개별매각|매각\s*공고|매각공고)\b",
  # 32개 호실 일괄매각 / 46개호실 일괄매각
  r"\b\d+\s*개\s*호실?\s*(?:일괄매각|개별매각|매각\s*공고|매각공고)\b",
  r"\b\d+\s*호실?\s*(?:일괄매각|개별매각|매각\s*공고|매각공고)\b",
  # N세대 개별매각
  r"\b\d+\s*세대\s*(?:일괄매각|개별매각|매각\s*공고|매각공고)\b",
  # N개필지 일괄매각
  r"\b\d+\s*개\s*필지\s*(?:일괄매각|개별매각|매각\s*공고|매각공고)\b",
  # (도합6호실, 근린생활시설) 매각 공고  ← 괄호 설명 + 매각 공고
  r"\(.*?도합\d+\s*호실.*?\)\s*(?:일괄매각|개별매각|매각\s*공고|매각공고)\b",
  # “재공매/재매각 공고” 류
  r"(?:재공매|재매각)\s*공고\b",
]

_SALE_RX = re.compile("|".join(f"(?:{p})" for p in _SALE_PATTERNS))

def extract_sale_content(title: str) -> str:
  """
  title에서 1)주소 2)광역/도+시군구 3)건물명을 제거한 '나머지'를 반환.
  - 선두의 '외', '외1', '외 3' 같은 접두는 추가로 제거.
  - 공백/구두점 정리.
  """
  s = (title or "").strip()

  # 1) 주소 제거
  addr = extract_address(s)
  if addr:
    s = s.replace(addr, "", 1).strip()

  # 2) 광역/도 + 시군구 제거
  prov_sgg = extract_province_sgg(title, use_address_fallback=True)
  if prov_sgg:
    s = s.replace(prov_sgg, "", 1).strip()

  # 3) 건물명 제거
  bld = extract_building_name(title)
  if bld:
    s = s.replace(bld, "", 1).strip()

  # ── 선행 '외' 접두 정리 ──
  s = re.sub(r"^외\s*\d+\s*", "", s)  # '외1', '외 3' 등
  s = re.sub(r"^외\s+", "", s)        # 단독 '외'

  # 공백/구두점 정리
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"\s*([,\u00B7·/])\s*", r"\1", s)
  s = re.sub(r"^[,·/]+", "", s)
  s = re.sub(r"[,·/]+$", "", s)

  return s.strip()
