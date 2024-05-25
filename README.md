# Projet finance 2024 PYBD, Groupe SMV

## Description
Ce projet a pour but de créer un programme de gestion de portefeuille d'actions. Il permettra de suivre l'évolution de la valeur des actions, de les acheter et de les vendre. Il permettra également de suivre l'évolution de la valeur du portefeuille.

## Quick start
- Temps estimé pour analyzer: 1h15
- A run en salle CISCO, NixOS Majeures qui a un dossier /srv/libvirt-workdir
- unzip boursorama.zip dans /srv/libvirt-workdir, il devrait y avoir /srv/libvirt-workdir/boursorama/20*
- supprimer boursorama.zip pour libérer de l'espace
- (Nos tests ont été fait avec le projet dans l'afs)

### Lancer analyzer

Dans bourse/docker/analyzer et bourse/docker/dashboard:
```bash
make
```

Modifier les paths dans bourse/docker-compose.yml si besoin. (déjà configuré pour /srv/libvirt-workdir/timescaledb et boursorama à /srv/libvirt-workdir/boursorama)

Lançer l'ensemble des services:
```bash
docker-compose up
```
Peut parfois crash, dans ce cas, à cause de podman, dans ce cas:
```bash
docker-compose down # dans un autre terminal
```
CTRL+C puis retenter

## Membres du groupe
- Juliette JIN <juliette.jin@epita.fr>
- Loris LIN <loris.lin@epita.fr>
- Alexandre WENG <alexandre.weng@epita.fr>

## Analyzer

- Pour accélérer le processus, les fichiers sont gérés dizaines de jour par dizaines (01-09, 10-19, 20-31), pour économiser du temps processing qui sont les mêmes pour chaque fichiers et pour ne pas surcharger la RAM (mois par mois fait planter). On le fera par ordre chronologique pour gérer les possibles changements de noms d'entreprises. (Le batch minimum serait de 1 jour pour pouvoir calculer les volumes de manière correcte)

- Les marchés (compA, compB, pea-pme, amsterdam...) ne sont pas pris en compte. On considère que des actions avec le même symbole sont la même entreprise et la même action. compA, compB et pea-pme ne représentant pas des marchés, il a été décidé de laisser la colonne market vide. Elle pourra être remplie si besoin, mais n'est pas utile pour le moment.

- Le processing se fait sur tous les fichiers du mois sans distinction du 'marché' que nous considérons comme inutile.

- Si on a des données provenant de fichiers différents pour le même symbole pour le même horaire on prend la moyenne des valeurs. (exemple: 2 fichiers du 01/01/2000 10h01:00.013 qui nous donnent 10.01 et 10.02 pour la valorisation d'un même symbole, on prendra 10.015.)

- Les données initialles des fichiers donnent les volumes cumulés sur la journée, on choisit de calculer le volume échangé depuis le dernier datapoint plutôt, pour avoir moins de données incohérentes.
- Les volumes échangés négatifs sont ignorés, car incohérents.
- Les volumes échangés et valeurs dépassant un INT32 bit sont ignorés car impossibles à stocker dans la DB sans changer le type de la colonne.

- Des entreprises avec un symbole finissant par NV (nouvelle valeur) peuvent souvent avoir des volumes nuls et aucune fluctuation de valeur tout le long de 2019 a 2024. On ignore et ne stocke pas les symboles avec un écart-type de 0 sur leur valeur.

- Nos entreprises sont identifiées par leur symbole. Si une entreprise change de nom, on prendra le nom le plus récent, à condition qu'il ne commence pas par SRD (Service de Règlement Différé). Le SRD est un service de la bourse sur une action, ce n'est pas un véritable changement de nom d'entreprise.

- Pour daystocks, dans le cas où une action aurait un volume échangé cumulé de la journée supérieur à la limite d'un INT32, on décide de stocker -1 dans la colonne volume, pour indiquer que le volume est trop grand pour être stocké et ne pas perdre les autres informations de la ligne.

- Pour insérer les données plus rapidement dans la base de données, on fait du multiprocessing.

- Les daystocks sont calculés et stockés avant le stockage de stocks pour ne pas fausser les données.

- Dans la mesure où le projet doit pouvoir tourner sur les machines de l'école et qu'ils ont un espace de stockage réduit, nous avons fait du resampling par heure pour réduire la quantité de données stockés dans stocks (après stockage de daystocks dans la DB).

- Et nous avons fait un drop_duplicate sur le df de stocks (après stockage de daystocks dans la DB) toujours pour gérer le problème de stockage des PC du CRI.

- Les jours fériés ne sont pas pris en compte.

## Dashboard (Inspiré de l'interface de TradingView)

- N'affiche pas les weekends, les jours fériés, car la bourse est fermée ces jours là. Cela permet d'avoir des graphiques plus lisibles.
- Pour la même raison, les heures entre 18h et 9h ne sont pas affichées.
- Ajout du choix de la période de temps pour avoir des bougies qui peuvent représenter des jours, semaines, mois, années.

## Utilisation

![alt text](https://github.com/Erdnaxela3/PYBD-2024/blob/main/frontend_instructions.png)
