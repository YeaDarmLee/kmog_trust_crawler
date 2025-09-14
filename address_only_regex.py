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

def extract_building_name(title: str) -> str:
  """
  주소/동·층·호/공고 키워드를 제거하고 남는 구간에서
  가장 그럴듯한 '건물명(동호수 제외)'을 한 개 반환.
  """
  s = (title or "").strip()

  # 1) 주소를 걷어낸 나머지 텍스트 구하기
  addr = extract_address(s)
  tail = s[len(addr):].strip() if addr and s.startswith(addr) else s

  # 2) 괄호/동층호/불용어(공고키워드) 제거
  tail = _BRACKET.sub(" ", tail)
  tail = _UNIT_TOKENS.sub(" ", tail)
  # 공고 키워드 이후는 잘라낸다(뒤는 매각내용일 확률↑)
  mstop = _BUILDING_STOP.search(tail)
  if mstop:
    tail = tail[:mstop.start()].strip()

  # 3) 쉼표/구분자 분해 후, 빌딩 후보 토큰 스캔
  parts = [p.strip() for p in _SEPARATORS.split(tail) if p.strip()]
  best = ""
  for p in parts:
    # 너무 짧은 일반명사는 제외(2자 이하)
    cand = []
    for mm in _BUILDING_CANDIDATE.finditer(p):
      token = mm.group(1).strip("-·").strip()
      if len(token) >= 3:
        cand.append(token)
    # 후보 중 가장 긴 것을 선택(힌트 접미사 포함 시 가산)
    if cand:
      cand.sort(key=lambda x: (x.endswith(tuple(
        ["타워","팰리스","캐슬","밸리","스퀘어","시티","힐스","아이파크","자이","더힐","프라자","프라임","스카이","에버빌","해링턴타워","해링턴","아파트","오피스텔","빌라","빌리지","몰","스포츠몰"]
      )), len(x)), reverse=True)
      pick = cand[0]
      if len(pick) > len(best):
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
  4) 매각내용: title 에서 1)주소, 2)시군구(광역/도+시군구),
  3)건물명을 제거하고 남은 텍스트를 그대로 반환.
  - 제거는 '처음 등장하는 1회'만 수행해 과제된 순서를 보존
  - 선행 공백/구두점 정리
  """
  s = (title or "").strip()

  # 1) 주소 제거
  addr = extract_address(s)
  if addr:
    # 주소가 선두에 오지 않는 예외도 있어서 첫 1회만 치환
    s = s.replace(addr, "", 1).strip()

  # 2) 시군구(광역/도 + 시군구) 제거
  prov_sgg = extract_province_sgg(title, use_address_fallback=True)
  if prov_sgg:
    s = s.replace(prov_sgg, "", 1).strip()

  # 3) 건물명 제거 (있을 때만)
  bld = extract_building_name(title)
  if bld:
    s = s.replace(bld, "", 1).strip()

  # 앞뒤 구두점/불필요 토큰 정리
  # - 남는 괄호쌍/콤마/슬래시 앞뒤 공백 정규화
  # - 선두·말미에 붙은 구분자 제거
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"\s*([,\u00B7·/])\s*", r"\1", s)     # 구분자 주변 공백
  s = re.sub(r"^[,·/]+", "", s)                    # 선두 구분자
  s = re.sub(r"[,·/]+$", "", s)                    # 말미 구분자
  s = s.strip()

  return s
