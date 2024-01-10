#  (2024-01-09)


### Bug Fixes

* **ipplist:** add opposed subject filter for ipplist resource ([e357635](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/e357635fa32df2cb9f86175b8fe4090c5bbe5a9c))
* **temporalconstraint:** check intersection instead of ordered slice match for criteria group tmp constraint ([546139a](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/546139ac1377db29ec5ae3595188e3204ddc41d9))


### Features

* add count with organization details ([f34f5f4](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/f34f5f43caa0cae31ddf0a0e42a1329047fc6752))
* **jobs:** add new callbackPath arg for callback overriding ([33eebd7](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/33eebd7fc7ca5c0213b0caf9fd4d04b02b855e64))
* **queryparser:** add raw query log ([f64ea49](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/f64ea497b7cc8a91cbde6c81c946a84561d866fa))



# [2.3.0](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/2.2.0...2.3.0) (2023-10-13)


### Bug Fixes

* column encounter date mapping ([f313967](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/f3139674354d955017ac3b17e6cf2eb2c3c5bb75))
* name of observation date col ([fa390bf](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/fa390bf1fbf30a26a7c453929688f7531c8c0c48))


### Features

* add total count processing time ([c7866cf](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/c7866cfffdf98810e226489dbe057167c60015dd))
* **imaging:** add imaging resource config ([aa3f64d](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/aa3f64d7153ea9c8a428711968511c4142ddbf7d))
* **job:** add error message ([9451b09](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/9451b09f39bb8966acf8662322a65712c9109cec))
* **jobs:** add optional PATCH callback url for jobs result ([a736037](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/a73603752e028484061c03db32d20b6a1027395b))
* **jobs:** change job result format ([08afdaf](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/08afdaf813745ab3a2940ce31672693190c2ac09))
* update create mode to support basic resource filter cohort creation ([d26e13c](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/d26e13cf20ea08a303b3abc169c9d7d811cf8488))



## [2.1.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/2.1.0...2.1.1) (2023-08-11)


### Bug Fixes

* add last update datetime to group aphp solr insert ([6ab6a1f](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/6ab6a1fdc423eb9b510917b22266cce3aff7dc91))
* cast column to timestamp for interval to work in temporal constraint ([aa92ca3](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/aa92ca31e14c132f73c934caab40473a43b92af0))
* change job status result to a list ([4e486ed](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/4e486edd8020edae2f75e0d9d0f44d9be110b6db))
* **config:** readd composition collection mapping for backward compatibilty ([37b0a3c](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/37b0a3c63c9a32656955456aede25de971596850))
* encounter patient col name ([991d395](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/991d39537a3f107de0809de6db7ba1753f1db497))
* encounter patient col name reference refactor r4 ([3dcd5dc](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/3dcd5dcd22d84909ed13afb90b5fe820676a7d5b))
* encounter patient col name reference refactor r4 ([f8a9a09](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/f8a9a09774d8e5db2637e2441a32bd8fc47ecf9e))
* **jobmanager:** remove option type in job status message ([2cf1bdd](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/2cf1bddc2b588769886bbbfaddfff3bea995df28))
* large cohorts are linked to patients in postgresql ([6cb5a64](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/6cb5a643e20f4ff10879be1e7f33fe64d4db819c))
* **pgtool:** set a writable tmp path ([58dda6a](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/58dda6a4f0dee980a9d50aabc51fdca20f9ee58c))
* readd count_all mode job routing ([72d0cb4](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/72d0cb45c636f98908e735e51513a86c4f371f91))
* result value in failed jobs ([ea75021](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/ea75021077ff1e665c248b840da0e644af83ee4f))
* set new spark user ([0195807](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/0195807ca8b2531ac6afc82242ad8e64848396c7))
* sql IS NULL replaced with column dsl ([9365aca](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/9365aca946aadc751c1c398e63ab4e9dada9c0f5))


### Features

* add json logging ([f2aecf9](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/f2aecf9e5b0c9f912545d9fcdfd36ffd433eddca))
* **parser:** relocate id based temporal constraints to proper sub group ([13cf615](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/13cf61522a612ebe056ecebc0217251ab637eab2))
* set spark scheduling to FAIR ([6698247](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/669824758a9cf7033429f9d6a6e900d2f0a03aa7))
* **spark:** add fair scheduled pool ([73acb61](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/73acb61595e62ecfb7dc73363999c687ad4557c0))



## [1.14.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.14.0...1.14.1) (2023-03-03)


### Features

* Version 1.14.1 ([e6b9da3](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/e6b9da30b576f34d4b5446ed1c5a2c5b932d7b6c))


### Reverts

* Revert "Remove the django response" ([a9a8897](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/a9a88975a0ae39d17604d0df49b5b730338c3324))



# [1.13.0](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.11.1...1.13.0) (2023-02-08)



## [1.11.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.11.0...1.11.1) (2023-01-11)



# [1.11.0](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.10.1...1.11.0) (2022-12-19)


### Reverts

* Revert "[1.11.0] #1761 - Fix date range list filter condition" ([047616a](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/047616a2e78897b4a855f0d2ca8cdced75e0a9e2))
* Revert "test rows increase" ([c5a1a45](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/c5a1a45f50eff0956c1d5a2b8d908bade2d2cc72))



## [1.10.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.10.0...1.10.1) (2022-12-14)



# [1.10.0](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.9.0...1.10.0) (2022-12-14)



# [1.9.0](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.8.0...1.9.0) (2022-11-17)



# [1.8.0](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.7.2...1.8.0) (2022-11-17)



## [1.7.2](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.7.1...1.7.2) (2022-09-22)



## [1.7.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.7.0...1.7.1) (2022-09-15)



## [1.6.5](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.6.4...1.6.5) (2022-08-23)



## [1.6.4](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.6.3...1.6.4) (2022-08-22)



## [1.6.3](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.6.2...1.6.3) (2022-08-11)



## [1.6.2](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.6.1...1.6.2) (2022-08-11)



## [1.6.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.6.0...1.6.1) (2022-08-11)



## [1.5.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.5.0...1.5.1) (2022-07-08)



## [1.4.3](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.4.2...1.4.3) (2022-06-29)



## [1.4.2](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.4.1...1.4.2) (2022-06-29)



## [1.4.1](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.4.0...1.4.1) (2022-06-29)



## [1.3.4](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.3.3...1.3.4) (2022-06-16)



## [1.3.3](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/compare/1.3.0...1.3.3) (2022-06-15)


### Reverts

* Revert "[934] sjs2 : init" ([f46730d](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/f46730d424473a0c60d35860e0fb24457e39983e))
* Revert "[934] - add logger" ([cb94050](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/cb940507f0fc481f9c82eca55b18cffdba542a65))
* Revert "[934] - add logger" ([6457a0b](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/6457a0b49788eeacc6143377c4c91eb162b4e5a0))
* Revert "[934] - add logger ++" ([e2c937b](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/e2c937b09a9cb343f5d53e028fe5222d6c302c45))
* Revert "[934] - add logger step 3" ([d65e6ef](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/d65e6ef25b3c85571f2f81ea762ac9d1d5c890af))
* Revert "[934] - add logger step 5" ([05614ed](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/05614eddeb3b464e240a4d040efd79bbac1d2462))
* Revert "[934] - add logger step 5 ++" ([e1b1e6f](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/e1b1e6fd765ea63e47f0e7f72ff3264030dba464))
* Revert "[934] - add logger step 5 +++" ([da1147e](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/da1147e78f1c75a6a1000da44e4e8e79916daa40))
* Revert "[934] - add logger step 5 +++" ([dea27f9](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/dea27f96c707b06e30bc18d49f0a9f555f57249c))
* Revert "[934] - test fix" ([a1be0f2](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/a1be0f26a6d3fc5bfa52dac3163b5c8968cb8346))
* Revert "[934] sjs2 : init" ([5cde283](https://gitlab.eds.aphp.fr/dev/cohort360/spark-job-server/commit/5cde2832a8c487c3be7ac14c9c04b06c8f02c350))


