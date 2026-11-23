# gateway-api-conformance-images

Repository for the images used on Gateway API conformance tests.

## Images

| Image | Source | Purpose |
|-------|--------|---------|
| `registry.k8s.io/gateway-api/conformance/echo-basic` | [`echo-basic/`](echo-basic/), [`docker/Dockerfile.echo-basic`](docker/Dockerfile.echo-basic) | HTTP/gRPC/TCP/UDP echo backend that reports the request it received |
| `registry.k8s.io/gateway-api/conformance/echo-advanced` | [`docker/Dockerfile.echo-advanced`](docker/Dockerfile.echo-advanced) | Istio echo backend used by the mesh tests |
| `registry.k8s.io/gateway-api/conformance/otel-sink` | [`otel-sink/`](otel-sink/), [`docker/Dockerfile.otel-sink`](docker/Dockerfile.otel-sink) | OTLP trace receiver that logs one `SPAN {...}` JSON line per span, used by the `TelemetryPolicy` tests |

See [RELEASE.md](RELEASE.md) for how images are built, promoted and consumed by
[kubernetes-sigs/gateway-api](https://github.com/kubernetes-sigs/gateway-api).

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Slack channel](https://kubernetes.slack.com/messages/sig-network-gateway-api)
- [Mailing List](https://groups.google.com/a/kubernetes.io/g/sig-network)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).

[owners]: https://git.k8s.io/community/contributors/guide/owners.md
[Creative Commons 4.0]: https://git.k8s.io/website/LICENSE
