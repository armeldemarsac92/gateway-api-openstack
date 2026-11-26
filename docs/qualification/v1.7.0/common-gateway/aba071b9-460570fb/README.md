# GATEWAY-HTTP Extended on controller aba071b9

The Standard bundle passed 60 of 63 selected Extended cases with the controller's 10-minute OpenStack resync. Three cases failed, zero were skipped, and both the upstream suite and the fixture exited 1. This selection excludes the 40 Core cases. [Original upstream report](http-standard-extended.yaml) · [Per-case verdicts, inputs and runtime observations](http-standard-extended.json)

| Failed case | Previous run on `454c1541` |
| --- | --- |
| `BackendTLSPolicyInvalidCACertificateRef` | Passed |
| `BackendTLSPolicySANValidation` | Failed |
| `ListenerSetAllowedRoutesNamespaces` | Passed |

The earlier run with the same 10-minute interval also passed 60 of 63. Two cases that previously failed passed on this run, `BackendTLSPolicy` and `BackendTLSPolicyInvalidKind`. [Previous upstream report](../454c1541-460570fb/http-standard-extended-operator-resync.yaml)

## Route deletion

All three cases passed their traffic and status assertions before invoking cleanup. The upstream suite then waited 30 seconds for Kubernetes to return `NotFound` for each HTTPRoute. HAProxy removed the selected paths during those windows, and the final inventory contained zero HTTPRoutes. The poll recorded no `NotFound` before its deadline. The available trace cannot distinguish a route still present from a GET error. For the negative SAN probes, HTTP 200 fails the test, and transport errors and HTTP 408 or 504 trigger retries. A 404 satisfies the assertion. The frontend had not yet published those paths, and the log omits the received status codes. [Upstream deletion check](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/kubernetes/apply.go#L221-L236)

| HTTPRoute | Seconds from logged deletion request to removal of its last selected path in HAProxy |
| --- | ---: |
| `gateway-conformance-infra/backendtlspolicy-san-test` | 4.975 |
| `gateway-conformance-infra/backendtlspolicy-invalid-ca-certificate-ref` | 15.288 |
| `gateway-api-ls-cross-ns/route-in-listenerset-namespace` | 17.044 |

## Test environment

The unmodified v1.7.0 conformance executable exercised a kind cluster with the Standard CRDs. The controller process used the binary from the signed image. The local environment used the Octavia API, the Amphora renderer and HAProxy. Fixtures supplied Keystone identity, Neutron discovery, VIP allocation and Amphora transport. The operator enabled closed-listener policy removal. The record includes the controller and wheel revisions, image digest, executable hashes and discovered CRDs. A deployed Hibiscus qualification is still open. [Controller release](https://github.com/armeldemarsac92/gateway-api-openstack/releases/tag/hibiscus-dev-aba071b9416112dd29e5852664913d3470013202) · [Regional qualification ledger](../../live-runs.md)

## Résultat Experimental

Le 26 novembre 2026, la sélection locale GATEWAY-HTTP Extended v1.7.0 a réussi **61 cas sur 64**, avec trois échecs et aucun cas ignoré dans le sous-ensemble Extended. La suite, le lanceur et la fixture ont chacun renvoyé `1`. L’essai a utilisé le manager issu de l’image OCI signée de `aba071b9`, Octavia `460570fb`, la resynchronisation OpenStack par défaut de 10 minutes et la suppression des politiques de listener fermé en Amphora. L’inventaire Kubernetes correspond aux 14 CRD du bundle Experimental, dont quatre du groupe `gateway.networking.x-k8s.io`. [YAML upstream original](http-experimental-extended.yaml) · [Verdicts, révisions et empreintes](http-experimental-extended.json) · [Source Octavia](https://github.com/armeldemarsac92/octavia/commit/460570fb9060fafcedba2907f7a24dc6ab83e7e8) · [Bundle Experimental v1.7.0](https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/experimental-install.yaml)

La référence Experimental à 10 minutes sur `454c1541` a expiré avec le code watchdog `124`, sans YAML permettant de comparer les verdicts. Sur `aba071b9`, par rapport à la cellule Standard, `BackendTLSPolicyInvalidCACertificateRef` et `ListenerSetAllowedRoutesNamespaces` réussissent en Experimental tandis que `BackendTLSPolicy` et `ListenerSetHTTPRouting` échouent. `BackendTLSPolicySANValidation` échoue sur les deux canaux, sur le nettoyage en Standard et sur le trafic en Experimental. Le cas supplémentaire `HTTPRouteInvalidParentRefNotMatchingListenerPort` réussit en Experimental. Ces résultats décrivent deux exécutions locales. La qualification sur un déploiement Hibiscus reste ouverte. [YAML Standard](http-standard-extended.yaml) · [YAML Experimental](http-experimental-extended.yaml) · [Qualification régionale](../../live-runs.md)

### BackendTLSPolicy et BackendTLSPolicySANValidation

Les trois sondes positives en échec font un GET en HTTP sur le listener de `same-namespace`. HAProxy doit ensuite joindre le backend en TLS. La suite attend trois réponses HTTP 200 consécutives dans une fenêtre de 30 secondes. Chaque sonde a reçu 297 réponses HTTP 404 et totalisé zéro succès. Les vérifications Kubernetes de la suite ont atteint les conditions attendues avant les GET. Au premier GET, le watch voyait les politiques `Accepted=True` et `ResolvedRefs=True` ainsi que le Gateway `Programmed=True`. Les deux HTTPRoutes avaient `Accepted=True` et `ResolvedRefs=True`, avec la condition propre au contrôleur `gateway-api-openstack.github.io/Programmed=False`, raison `Pending`. [Test BackendTLSPolicy](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/backendtlspolicy.go#L96-L117) · [Test SAN](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/backendtlspolicy-san.go#L60-L103) · [Délais et convergence](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/config/timeout.go#L196-L223)

| Cas | GET positif | Premier GET → expiration (UTC) | Premier chargement HAProxy observé avec le chemin sur `same-namespace` |
| --- | --- | --- | --- |
| `BackendTLSPolicy` | `/backendtlspolicy` | 14:48:41.227 → 14:49:11.228 | Aucun avant la suppression. Le même chemin était actif dès 14:48:35.594 sur le Gateway HTTPS du sous-test de rechiffrement, qui a réussi. |
| `BackendTLSPolicySANValidation` | `/backendtlspolicy-san-dns` | 14:48:41.386 → 14:49:11.387 | 14:49:36.289, environ 25 s après l’expiration. |
| `BackendTLSPolicySANValidation` | `/backendtlspolicy-san-uri` | 14:49:11.392 → 14:49:41.392 | 14:49:45.704, environ 4 s après l’expiration. |

Le sous-test avec plusieurs SAN a réussi après 136 réponses 404 puis trois réponses conformes, environ 80 ms après le chargement du chemin à 14:49:55.088 UTC. Les sous-tests négatifs de nom, de certificat et de SAN ont satisfait leur assertion. Celle-ci accepte un HTTP 404, et les traces ne donnent pas le code HTTP des réponses acceptées pour ces sondes négatives. Aucun échange TLS côté backend n’a été capturé dans ces fenêtres. Le délai de publication des chemins figure dans la trace. La cause de ce délai et le résultat qu’aurait donné une nouvelle sonde DNS ou URI après la publication restent inconnus. Le watch a ensuite observé la suppression des deux HTTPRoutes, environ 11 s et 16 s après les demandes de suppression. [Assertion HTTP négative](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/http/http.go#L331-L352)

### ListenerSetHTTPRouting

Les vérifications de statut du Gateway, des listeners et des parents HTTPRoute ont abouti, puis les 36 sous-tests de trafic ont journalisé `Request passed`. Lors du nettoyage, la suite a demandé la suppression de `gateway-conformance-infra/listener-set-http-routing-1-route` à 14:53:43.892 UTC. Le chemin de cette route a disparu de la configuration HAProxy observée à 14:54:03.638 UTC, environ 19,75 s plus tard. Après environ 30 secondes, la suite a consigné `context deadline exceeded`. La demande de suppression suivante, celle de `gateway-section-route`, est horodatée à 14:54:13.898 UTC. L’attente se termine lorsqu’une lecture Kubernetes renvoie `NotFound`. Le poll ne journalise pas les réponses intermédiaires, et le watch ciblé ne suivait pas cette route. Les traces laissent donc ouvertes la présence de la route pendant l’attente et une erreur de lecture Kubernetes. L’inventaire final comptait zéro HTTPRoute. [Test de routage](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/tests/listenerset-http-routing.go#L44-L86) · [Vérification de suppression](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/kubernetes/apply.go#L221-L236)

Le récapitulatif Go omet la ligne finale du cas, et le parseur de la fixture le classe `NOT-RUN`. Le journal contient son exécution et l’erreur de nettoyage attribuée à ce cas. La suite enregistre le résultat après les callbacks de nettoyage et le YAML upstream le compte en échec, ce que reprend le JSON. [Enregistrement des résultats](https://github.com/kubernetes-sigs/gateway-api/blob/e26d183dbbc1bb782b78addbd26faba03955c152/conformance/utils/suite/suite.go#L532-L579)

La fixture a aussi journalisé un HTTP 500 sur une mise à jour de listener via l’API Octavia à 14:52:19.063 UTC. La trace de rendu indique que la fixture a rejeté une demande de retrait du service HAProxy pendant une activation de politique. Les traces disponibles ne relient pas ce rejet aux trois verdicts de la suite. [Empreintes du journal de la fixture et des requêtes Octavia](http-experimental-extended.json)
