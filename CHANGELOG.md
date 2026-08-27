# Changelog

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
