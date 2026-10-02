# arcus-python-client

[English](README.md) | **한국어**

[Arcus 캐시](https://github.com/naver/arcus)를 위한 Python 클라이언트입니다.

## 요구 사항

- Python 3.11 이상.
- 네트워크 클라이언트를 실행할 Linux 환경. 현재 `select.epoll()`을 사용합니다.
- ZooKeeper에 등록된 Arcus 캐시 서비스.

패키지에는 Kazoo가 런타임 의존성으로 선언되어 있습니다. 외부 서비스가 필요 없는
단위 테스트는 다른 운영체제에서도 실행할 수 있지만, 이 테스트가 통과했다고 해서
해당 운영체제에서 네트워크 클라이언트를 지원한다는 뜻은 아닙니다.

## 설치

저장소를 내려받은 디렉터리에서 설치합니다.

```sh
python -m pip install .
```

또는 아래 빌드 절차로 생성한 wheel을 설치합니다.

```sh
python -m pip install dist/arcus_python_client-1.0.0-py3-none-any.whl
```

배포 파일에는 `arcus` 패키지와 기존 코드의 호환성을 위한 작은 `arcus_mc_node`
모듈이 포함됩니다. `arcus_mc_node`는 사용을 권장하지 않는 deprecated 모듈이며,
기존 import는 경고와 함께 계속 동작합니다. wheel을 생성하는 것만으로 패키지
인덱스에 릴리스가 게시되지는 않습니다.

## 프로젝트 구조

```text
src/arcus/             # 클라이언트 패키지
  api/                 # 키/값, List, Set 및 B+Tree API
  protocol/            # Arcus 프로토콜과 네트워크 전송
tests/                 # 단위 테스트
  integration/         # 실제 Arcus와 ZooKeeper를 사용하는 테스트
  legacy/              # 기존 수동 스모크 테스트 스크립트
tools/                 # 독립 실행형 관리 유틸리티
benchmarks/            # 성능 비교와 프로파일링
stability/             # 장애, 복구 및 지속 부하 실험
docs/                  # 아키텍처, 마이그레이션 및 검증 가이드
```

내부 책임 분담과 요청 흐름은 [아키텍처 가이드](docs/architecture.md),
API 변경 사항과 로깅 설정은 [마이그레이션 가이드](docs/migration.md)를 참고하세요.

## 사용 방법

ZooKeeper 주소(`localhost:2181`)와 서비스 코드(`test`)를 테스트 환경에 맞게 바꿔 실행합니다.

```python
from arcus import Arcus, ArcusLocator, ArcusMCNodeAllocator, ArcusTranscoder

allocator = ArcusMCNodeAllocator(ArcusTranscoder())
client = Arcus(ArcusLocator(allocator))
client.connect("localhost:2181", "test")
try:
    client.kv.set("example:key", "hello", exptime=60).get_result(timeout=5)
    print(client.kv.get("example:key").get_result(timeout=5))
finally:
    client.disconnect()
```

키/값은 `client.kv`, List는 `client.lop`, Set은 `client.sop`, B+Tree는 `client.bop`을
사용합니다. 명령 실행 결과는 `get_result()`로 받습니다.

타임아웃은 [설정과 오류 처리](docs/configuration.ko.md), 기존 API와 로깅은
[마이그레이션 가이드](docs/migration.md)를 참고하세요.

## 개발

가상 환경을 만들고 개발 의존성을 설치합니다.

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
```

Python 파일의 코드 포맷은 Ruff로 맞춥니다. 들여쓰기는 공백 4칸, 문자열은 큰따옴표,
목표 줄 길이는 88자, 줄바꿈은 LF를 사용합니다.

```sh
ruff format .
ruff format --check .
```

Arcus나 ZooKeeper 없이 단위 테스트를 실행합니다.

```sh
python -m pytest --timeout=30
```

Docker Engine과 Docker Compose v2로 Linux 통합 테스트를 실행합니다.

```sh
./scripts/test-integration.sh
```

이 스크립트는 격리된 ZooKeeper 서비스와 Arcus 노드 두 개를 생성하고, Linux
컨테이너에서 wheel을 빌드·설치한 다음 기본 API와 동시 요청을 테스트합니다.
결과를 `build/integration/`에 저장하고 테스트 환경을 제거합니다. 버전, 제한 사항,
정리 절차 및 명령은 [통합 테스트 가이드](tests/integration/README.md)를 참고하세요.
통합 테스트는 기본 테스트 탐색에서 제외됩니다.

GitHub Actions에서는 Python 3.11–3.14 단위 테스트, 포맷 및 패키지 검사,
Docker 통합 테스트를 분리해 실행합니다. 단위 테스트 작업은 소스 체크아웃 밖에서
설치된 패키지를 대상으로 실행합니다.

테스트 전제와 통과 기준은 [검증 범위](docs/validation.md), 클라이언트 비교와
프로파일링은 [벤치마크](benchmarks/README.md), 격리된 환경의 장애 및 자원 검사는
[안정성 실험](stability/README.md)을 참고하세요. 짧은 로컬 측정만으로 운영 환경의
처리 용량이나 장기 안정성을 입증할 수는 없습니다. 압축과 사용자 정의 객체 직렬화는
아직 검증된 데이터 형식에 포함되지 않으며, 운영 워크로드가 요구 기준을 충족하는지도
별도로 검증해야 합니다.

## 배포 파일 빌드

소스 배포 파일과 wheel을 빌드한 뒤 메타데이터를 검사합니다.

```sh
python -m build
python -m twine check dist/*
```

`python -m build`는 기본적으로 소스 배포 파일에서 wheel을 빌드합니다. 릴리스하기
전에는 소스 체크아웃 밖의 깨끗한 환경에 wheel을 설치하고, 설치된 패키지를 대상으로
관련 테스트를 실행하세요.

패키지 버전은 `pyproject.toml`에 선언되어 있으며, 초기값은 `ChangeLog`에 있던
`1.0.0`을 따릅니다. 릴리스마다 이 버전을 갱신하고 사용자에게 영향을 주는 변경을
`ChangeLog`에 기록하세요. 사내 패키지 인덱스나 PyPI에 업로드하는 작업은 별도의
릴리스 단계이며, 사용할 인덱스와 인증 정보가 필요합니다.

## 관리 유틸리티

독립 실행형 관리 스크립트는 [tools/](tools/README.md)에서 계속 제공합니다.
클라이언트 패키지를 설치할 때 함께 설치되지는 않습니다. 의존성은 Kazoo와 `scramp`이며,
`arcus_cmd.py`에는 추가로 `paramiko`가 필요합니다. `arcus_util.py`를 사용하는
유틸리티는 Python 3.13에서 제거된 `telnetlib`를 import하므로 현재 Python 3.11 또는
3.12가 필요합니다. 이 제한은 패키지로 제공하는 핵심 클라이언트에는 적용되지 않습니다.

## 라이선스

[Apache License, Version 2.0](LICENSE)을 따릅니다.
