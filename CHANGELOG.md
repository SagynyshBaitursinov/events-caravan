# Changelog

## [2.0.0](https://github.com/SagynyshBaitursinov/events-caravan/compare/events-caravan-v1.2.1...events-caravan-v2.0.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* **dynamodb:** renamed sharded entity reference to sliced. Same for slicing entity stream.

### Features

* **autoconfig:** switch features on only when their trigger is present ([91affbb](https://github.com/SagynyshBaitursinov/events-caravan/commit/91affbb6955c50128f737638b20b2d1e08e9c12c))
* **core:** tell sequenced and unsequenced events apart by their reference ([64c7eeb](https://github.com/SagynyshBaitursinov/events-caravan/commit/64c7eeb397a769652bbe13349e493ccd43881f69))
* **dynamodb:** renamed sharded entity reference to sliced. Same for slicing entity stream. ([40ec0fa](https://github.com/SagynyshBaitursinov/events-caravan/commit/40ec0fa514b37b1e1abcb51f913b82b46300f3f6))
* **entity-stream:** fail startup on stream registrations without a writer ([85b7352](https://github.com/SagynyshBaitursinov/events-caravan/commit/85b7352c93a405f5298a9ba714c68b43bbc70a85))
* **events:** Adding possibility of NoPayload event options, that do not serialize payloads. ([4356141](https://github.com/SagynyshBaitursinov/events-caravan/commit/4356141d76fb0e9bb71b6dbbf383adf9e0193b0e))
* **producer:** publish unsequenced events straight to the message broker ([c7fd509](https://github.com/SagynyshBaitursinov/events-caravan/commit/c7fd509b68d0f4e7429e537443092b6b1cc4736f))
* **sns:** add SNS event message publisher and Spring Boot starter ([da71ab8](https://github.com/SagynyshBaitursinov/events-caravan/commit/da71ab89c537027835fcdb2324e81b785ef026e4))
* **sourcing:** make the snapshot store optional ([f732e53](https://github.com/SagynyshBaitursinov/events-caravan/commit/f732e53f1eb65f309f4009e709aa33415c2d4b18))
* **sqs-starter:** resolve subscribed queues from the EntityEventsRegistry ([ce9ffa6](https://github.com/SagynyshBaitursinov/events-caravan/commit/ce9ffa623751841041161d80ee8c2a3a3e6928bc))


### Bug Fixes

* **deps:** bump the maven-dependencies group with 2 updates ([#22](https://github.com/SagynyshBaitursinov/events-caravan/issues/22)) ([29196fb](https://github.com/SagynyshBaitursinov/events-caravan/commit/29196fb005f31fbfd73f0ef33135da351c68c089))
* **deps:** bump the maven-dependencies group with 4 updates ([#24](https://github.com/SagynyshBaitursinov/events-caravan/issues/24)) ([db2c961](https://github.com/SagynyshBaitursinov/events-caravan/commit/db2c9616007543020a2548a93f431519ee4450f9))

## [1.2.1](https://github.com/SagynyshBaitursinov/events-caravan/compare/events-caravan-v1.2.0...events-caravan-v1.2.1) (2026-10-31)


### Bug Fixes

* **deps:** bump dependencies and restructure poms ([beb1372](https://github.com/SagynyshBaitursinov/events-caravan/commit/beb13725189eb0935e44fc4511b7028b144dc094))
* **deps:** bump the maven-dependencies group with 6 updates ([#14](https://github.com/SagynyshBaitursinov/events-caravan/issues/14)) ([a4d3f7b](https://github.com/SagynyshBaitursinov/events-caravan/commit/a4d3f7b4619bf1760ede78a584c0a054d9e3856e))

## [1.2.0](https://github.com/SagynyshBaitursinov/events-caravan/compare/events-caravan-v1.1.1...events-caravan-v1.2.0) (2026-08-27)


### Features

* **entity-stream:** parametrize entity-stream sharding per entityName ([8c7f7d9](https://github.com/SagynyshBaitursinov/events-caravan/commit/8c7f7d9441990e2a661ee7b247859627958f3167))


### Bug Fixes

* correcting exception message ([c4af7cb](https://github.com/SagynyshBaitursinov/events-caravan/commit/c4af7cbc0f66fcd849266ae5c88687ab2d67a35e))
* **deps:** bump the maven-dependencies group with 5 updates ([#10](https://github.com/SagynyshBaitursinov/events-caravan/issues/10)) ([3b94fa9](https://github.com/SagynyshBaitursinov/events-caravan/commit/3b94fa9e478544951ddc04c4e1babdff2ae65167))
* **deps:** bump the maven-dependencies group with 5 updates ([#13](https://github.com/SagynyshBaitursinov/events-caravan/issues/13)) ([f01fc2a](https://github.com/SagynyshBaitursinov/events-caravan/commit/f01fc2afdd738b5dc657bba5ff6b85eaa278a2f9))
* **message-deletion:** Started long polling for messages to be deleted in smaller slices ([c8eed47](https://github.com/SagynyshBaitursinov/events-caravan/commit/c8eed47fcd10ef275ac8da9a5450114986bc650b))

## [1.1.1](https://github.com/SagynyshBaitursinov/events-caravan/compare/events-caravan-v1.1.0...events-caravan-v1.1.1) (2026-08-14)


### Dependencies

* 1.1.1-Snapshot ([2a52220](https://github.com/SagynyshBaitursinov/events-caravan/commit/2a522202ef9cbaa24698ea749b568bbce70b09ed))
* renameing events-caravan to events-caravan-core to be explicit ([a4263aa](https://github.com/SagynyshBaitursinov/events-caravan/commit/a4263aaf4b52331b13f9b06b9044337f41076082))

## [1.1.0](https://github.com/SagynyshBaitursinov/events-caravan/compare/events-caravan-v1.0.1...events-caravan-v1.1.0) (2026-08-11)


### Features

* Bringing an optional functionality of setting up an optional Entity stream ([0d6c02b](https://github.com/SagynyshBaitursinov/events-caravan/commit/0d6c02b3f3c529906517ba37985b4ebd239f76b5))


### Bug Fixes

* **ci:** use PAT instead of github token for Release please and dependabot. ([c2e620d](https://github.com/SagynyshBaitursinov/events-caravan/commit/c2e620d75dd3ea61c4ce9208abbdcc661db6b989))


### Performance Improvements

* skip guaranteed-empty page query when a shard ends exactly at its upper bound ([2ae0940](https://github.com/SagynyshBaitursinov/events-caravan/commit/2ae0940729ad97ded0db8dbc99687a141507cd72))

## [1.0.1](https://github.com/SagynyshBaitursinov/events-caravan/compare/events-caravan-v1.0.0...events-caravan-v1.0.1) (2026-08-09)


### Bug Fixes

* **ci:** Adding explicit permissions to ci.yml ([23aadfc](https://github.com/SagynyshBaitursinov/events-caravan/commit/23aadfc31eb485d62af410aa7ecef41e93a62780))
* **ci:** make dependabot commits use conventional commit prefixes for release-please ([0f0c3b7](https://github.com/SagynyshBaitursinov/events-caravan/commit/0f0c3b7b610c533f530f62728bb9762e59ac1362))
* **deps:** bump the maven-dependencies group with 2 updates ([#3](https://github.com/SagynyshBaitursinov/events-caravan/issues/3)) ([0e61d8d](https://github.com/SagynyshBaitursinov/events-caravan/commit/0e61d8d3b274cc9dad107841aa1c70bf1baabc50))

## 1.0.0 (2026-08-09)


### Bug Fixes

* **ci:** Adding explicit permissions to ci.yml ([23aadfc](https://github.com/SagynyshBaitursinov/events-caravan/commit/23aadfc31eb485d62af410aa7ecef41e93a62780))
* **ci:** make dependabot commits use conventional commit prefixes for release-please ([0f0c3b7](https://github.com/SagynyshBaitursinov/events-caravan/commit/0f0c3b7b610c533f530f62728bb9762e59ac1362))
