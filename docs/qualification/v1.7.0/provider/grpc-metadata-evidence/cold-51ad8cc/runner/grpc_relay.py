"""Route TCP to the unchanged grpc echo workloads in the disposable kind node."""
import sys
import kind_multi_relay as relay

relay.PEERS = {"tls-backend": (8443, 1), **{f"grpc-infra-backend-v{i}": (3000, 2) for i in (1, 2, 3)}}
raise SystemExit({"inner": relay.inner, "outer": relay.outer}[sys.argv[1]](sys.argv[2:]))
