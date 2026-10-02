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

다음 예제는 전용 테스트 서비스에서 실행하세요.

```python
from arcus import Arcus, ArcusLocator, ArcusMCNodeAllocator, ArcusTranscoder

allocator = ArcusMCNodeAllocator(
    ArcusTranscoder(), connect_timeout=1, io_timeout=1, operation_timeout=5
)
client = Arcus(ArcusLocator(allocator))
client.connect("localhost:2181", "test")
try:
    client.kv.set("example:key", "hello", exptime=60).get_result(timeout=5)
    assert client.kv.get("example:key").get_result(timeout=5) == "hello"
finally:
    client.disconnect()
```

키/값 명령은 `client.kv`, List는 `client.lop`, Set은 `client.sop`, B+Tree는
`client.bop`을 사용합니다. 예를 들어 `client.bop_delete(key, range)` 대신
`client.bop.delete(key, range)`를 호출합니다. 기존 메서드도
`DeprecationWarning`과 함께 계속 사용할 수 있습니다. 전체 API 대응 관계,
컬렉션 래퍼, 표준 `logging` 설정은
[API 마이그레이션과 로깅 설정](docs/migration.md)을 참고하세요.

`exptime`은 캐시 항목의 만료 시간입니다. 소켓 타임아웃이나 요청 결과의
`get_result()`에 전달하는 타임아웃과는 별개입니다. `get_result(timeout=5)`는
해당 호출자의 대기 시간을 5초로 제한하며, 그 안에 결과가 준비되지 않으면
`queue.Empty`를 발생시킵니다. 요청 작업 자체를 취소하지는 않으므로 나중에
`get_result()`를 다시 호출해 결과를 받을 수 있습니다. 기본값인 `timeout=0`은
요청 작업의 결과가 나올 때까지 기다립니다. 연결이 무효화되면 캐시 미스를 반환하는
대신 `ArcusNodeConnectionException`을 발생시킵니다.

allocator의 기본값은 연결 타임아웃 1초, 소켓 I/O 타임아웃 1초, 요청 제출 시점부터
계산하는 작업 완료 기한 5초입니다. 세 값 모두 유한한 양수여야 합니다. 소켓 또는
작업 타임아웃이 발생하면 `TimeoutError`를 발생시킵니다. 연결 실패로 다른 처리 중인
요청도 무효화될 수 있습니다. poller는 응답이 없는 연결을 주기적으로 확인합니다.
응답 파싱에 블로킹 I/O를 사용하므로 다른 노드의 처리가 기한 초과 감지를 늦출 수
있습니다. 따라서 이 설정이 모든 부하 조건에서 엄격한 종단 간 응답 시간 상한을
보장하는 것은 아닙니다.

실패했거나 일부만 전송된 요청은 자동으로 재전송하지 않습니다. `noreply` 요청은
소켓 쓰기가 성공하면 완료되지만, 서버에서 실행까지 끝났다는 뜻은 아닙니다.
`disconnect()`는 노드, 작업 스레드, epoll 및 ZooKeeper 자원을 해제합니다.
연결을 종료한 뒤 같은 클라이언트로 다시 연결할 수 있습니다.

최초 ZooKeeper 연결과 노드 조회는 합계 15초의 대기 제한을 공유합니다. 실패하면
클라이언트 자원을 해제합니다. 캐시 노드 연결과 정리에는 별도의 시간이 소요되며,
이 대기 제한은 allocator의 캐시 요청 제한과 별개입니다. 재연결 후에는 discovery가
비동기적으로 노드 구성을 갱신하므로, ZooKeeper 조회가 이미 알려진 캐시 노드 목록을
이용하는 요청 라우팅을 막지 않습니다.

기존 스모크 테스트 스크립트는 실제 서비스에서 기본 동작을 확인합니다.

```sh
python tests/legacy/client_smoke.py <ZOOKEEPER_HOSTS> <SERVICE_CODE>
```

이 스크립트는 고정된 테스트 키에 데이터를 쓰므로 격리된 서비스가 필요합니다.
스크립트를 import하는 것만으로도 실행되며, 기본 테스트 탐색에서는
`tests/legacy/`를 제외합니다.

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
