# Conservation des poids d’une HTTPRoute après un changement de ReferenceGrant

Sur le même Gateway, une HTTPRoute donne un poids de 3 au Service public et de 1 au Service d’un autre namespace. Le ReferenceGrant du namespace de destination autorise ce second Service. Le contrôle porte sur la même HTTPRoute, après convergence du contrôleur.

## Critères observables

| Transition | Statut du parent de la route | Réponses attendues |
|---|---|---|
| Retrait du ReferenceGrant | `Accepted=True`, `ResolvedRefs=False/RefNotPermitted` | Environ 75 % de HTTP 200 du Service public et 25 % de HTTP 500 sans appel applicatif. Zéro appel routé au Service protégé. |
| Rétablissement du même droit | `Accepted=True`, `ResolvedRefs=True/ResolvedRefs` | Environ 75 % de HTTP 200 du Service public et 25 % de HTTP 200 du Service protégé. Les 500 dus au retrait cessent. |

Aux deux transitions, l’UID, la génération et la spécification de la route restent identiques. Le listener compte une route attachée. La condition `gateway-api-openstack.github.io/Programmed` du fork revient à `True/Programmed` après convergence. Les deux cibles de la politique Octavia conservent les poids 3 et 1.

## Mesure locale

Chaque ligne regroupe 1 024 requêtes sur de nouvelles connexions et 1 024 sur une connexion persistante. Les essais utilisent le contrôleur extrait de l’image, l’API Kubernetes locale, l’API Octavia du fork et HAProxy. Les dépendances cloud sont simulées.

| Canal | Transition | HTTP 200 public | HTTP 200 protégé | HTTP 500 vide |
|---|---|---:|---:|---:|
| Standard | Après le retrait | 1545 | 0 | 503 |
| Standard | Après le rétablissement | 1554 | 494 | 0 |
| Experimental | Après le retrait | 1548 | 0 | 500 |
| Experimental | Après le rétablissement | 1527 | 521 | 0 |

Contrôleur `e6fbd1a853aceaed3a141b406d5860f5242b9ecb`, image linux/amd64 `sha256:d9e0126c2d7f46fa919fb99a8bba36c54c6570b6f7e05609d177e944ebea7c60`. Les mesures par mode de connexion, les conditions et les politiques relues figurent dans [le relevé Standard](standard.json) et [le relevé Experimental](experimental.json).

Les profils sur un cloud Hibiscus déployé restent à qualifier. [Procédure de l’essai 3:1](https://github.com/armeldemarsac92/gateway-api-openstack/blob/main/docs/qualification/v1.7.0/provider/opt-in-request-header-modifier.md#weighted-referencegrant-regression)
