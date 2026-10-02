# 타임아웃과 연결 관리

[English](configuration.md) | **한국어**

연결과 get/set 예제는 [README 사용 방법](../README.ko.md#사용-방법)을 참고하세요.

## 캐시 만료와 결과 대기

`exptime`은 캐시 항목의 만료 시간입니다. 소켓 타임아웃이나 요청 결과의
`get_result()`에 전달하는 타임아웃과는 별개입니다.

`get_result(timeout=5)`는 해당 호출자의 대기 시간을 5초로 제한하며, 그 안에
결과가 준비되지 않으면 `queue.Empty`를 발생시킵니다. 요청 작업 자체를 취소하지는
않으므로 나중에 `get_result()`를 다시 호출해 결과를 받을 수 있습니다. 기본값인
`timeout=0`은 요청 작업의 결과가 나올 때까지 기다립니다. 연결이 무효화되면 캐시
미스를 반환하는 대신 `ArcusNodeConnectionException`을 발생시킵니다.

## 캐시 요청 타임아웃

allocator를 생성할 때 캐시 요청의 제한 시간을 설정합니다.

```python
from arcus import ArcusMCNodeAllocator, ArcusTranscoder

allocator = ArcusMCNodeAllocator(
    ArcusTranscoder(), connect_timeout=1, io_timeout=1, operation_timeout=5
)
```

| 설정 | 기본값 | 의미 |
| --- | --- | --- |
| `connect_timeout` | 1초 | 캐시 노드 연결 타임아웃 |
| `io_timeout` | 1초 | 소켓 I/O 타임아웃 |
| `operation_timeout` | 5초 | 요청 제출 시점부터 계산하는 작업 완료 기한 |

세 값 모두 유한한 양수여야 합니다. 소켓 또는 작업 타임아웃이 발생하면
`TimeoutError`를 발생시킵니다. 연결 실패로 다른 처리 중인 요청도 무효화될 수
있습니다. poller는 응답이 없는 연결을 주기적으로 확인합니다. 응답 파싱에 블로킹
I/O를 사용하므로 다른 노드의 처리가 기한 초과 감지를 늦출 수 있습니다. 따라서 이
설정이 모든 부하 조건에서 엄격한 종단 간 응답 시간 상한을 보장하는 것은 아닙니다.

## 요청 실패와 자원 정리

실패했거나 일부만 전송된 요청은 자동으로 재전송하지 않습니다. `noreply` 요청은
소켓 쓰기가 성공하면 완료되지만, 서버에서 실행까지 끝났다는 뜻은 아닙니다.

`disconnect()`는 노드, 작업 스레드, epoll 및 ZooKeeper 자원을 해제합니다.
연결을 종료한 뒤 같은 클라이언트로 다시 연결할 수 있습니다.

## ZooKeeper 연결과 복구

최초 ZooKeeper 연결과 노드 조회는 합계 15초의 대기 제한을 공유합니다. 실패하면
클라이언트 자원을 해제합니다. 캐시 노드 연결과 정리에는 별도의 시간이 소요되며,
이 대기 제한은 allocator의 캐시 요청 제한과 별개입니다. 재연결 후에는 discovery가
비동기적으로 노드 구성을 갱신하므로, ZooKeeper 조회가 이미 알려진 캐시 노드 목록을
이용하는 요청 라우팅을 막지 않습니다.
