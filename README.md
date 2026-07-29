# StreamScope

## Screenshots

### Streams

![StreamScope Streams](./design/streamscope-light-streams.png)

### Redis Settings

![StreamScope Redis Settings](./design/streamscope-light-settings.png)

## Quick Start

```sh
docker compose up -d
```

브라우저에서 [http://localhost:8080](http://localhost:8080)을 열고 로그인합니다.

- ID: `admin`
- Password: `password`

로그인 후 Settings 화면에서 Redis 연결을 추가합니다. 관리자 비밀번호도 Settings에서 필요할 때 변경할 수 있습니다.

Docker Desktop에서 Mac 또는 Windows 호스트에 실행 중인 Redis를 연결할 때는 Redis 주소에 `localhost:6379` 대신 `host.docker.internal:6379`를 입력합니다.

## Docker Run

```sh
docker run -d \
  --name streamscope \
  -p 8080:8080 \
  -v streamscope-data:/data \
  ghcr.io/ghkdqhrbals/streamscope:latest
```

호스트의 `9090` 포트로 실행하려면:

```sh
docker run -d \
  --name streamscope \
  -p 9090:8080 \
  -v streamscope-data:/data \
  ghcr.io/ghkdqhrbals/streamscope:latest
```

Redis 설정을 실행 시 미리 전달하려면:

```sh
docker run -d \
  --name streamscope \
  -p 8080:8080 \
  -v streamscope-data:/data \
  -e REDIS_HOST=redis.example.internal \
  -e REDIS_PORT=6379 \
  -e REDIS_USERNAME=default \
  -e REDIS_PASSWORD=change-me \
  ghcr.io/ghkdqhrbals/streamscope:latest
```

Redis에 비밀번호가 없으면 `REDIS_PASSWORD`를 생략합니다. 전달한 Redis 설정은 `/data/config.properties`에 저장되므로 같은 볼륨을 다시 연결하면 환경변수 없이도 유지됩니다.

사용 가능한 실행 환경변수:

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `PORT` | `8080` | 컨테이너 내부 HTTP 포트 |
| `REDIS_ID` | `default` | 연결 ID |
| `REDIS_NAME` | `Redis` | 화면 표시 이름 |
| `REDIS_MODE` | `standalone` | `standalone`, `sentinel`, `cluster` |
| `REDIS_HOST` | 없음 | 단일 Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `REDIS_NODES` | 없음 | Cluster 노드 목록 (`host:port,host:port`) |
| `REDIS_URL` | 없음 | `redis://` 또는 `rediss://` URL |
| `REDIS_MASTER_NAME` | 없음 | Sentinel master 이름 |
| `REDIS_USERNAME` | 없음 | Redis ACL 사용자 |
| `REDIS_PASSWORD` | 없음 | Redis 비밀번호 |
| `REDIS_PASSWORD_FILE` | 없음 | Redis 비밀번호 파일 |
| `REDIS_DATABASE` | `0` | Redis database |
| `REDIS_KEY_PATTERN` | `*` | Stream 검색 패턴 |
| `REDIS_TLS` | `false` | TLS 사용 여부 |
| `REDIS_TLS_SERVER_NAME` | 없음 | TLS 서버 이름 |
| `REDIS_TLS_CA_FILE` | 없음 | CA 인증서 경로 |
| `REDIS_TLS_CERT_FILE` | 없음 | mTLS 인증서 경로 |
| `REDIS_TLS_KEY_FILE` | 없음 | mTLS 개인 키 경로 |

`REDIS_HOST`, `REDIS_NODES`, `REDIS_URL` 중 하나만 사용합니다. 전체 저장 형식과 기본값은 [config.properties](./config.properties)의 주석에서 확인할 수 있습니다.

로컬 소스에서 이미지를 직접 빌드하려면:

```sh
docker build -t streamscope:local .
```
