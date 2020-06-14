# RCP216

## Configuration

`src/main/scala/cnam/Conf.scala`

## Lancement

`spark-submit package.jar <interval> <history> <method>`

`interval`: Intervalle pour la génération du flux

`history`: Nombre de jours à conserver pour l'entraînement du modèle

`method`: Méthode à utiliser
- `auto` Méthode automatique basée sur `org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD`
- `manual` Méthode manuelle (ancienne api)
- `structured` Méthode manuelle (nouvelle api)

