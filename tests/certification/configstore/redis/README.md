# Redis Configuration Store certification testing

This project aims to test the Redis Configuration Store component under various conditions.

## Test plan

## Basic Tests:
1. Able to create and init.
2. Able to do fetch many keys at once.
3. Able to listen to update events.
4. Negative test to fetch configuration with key, that is not present.
5. Able to unsubscribe for update events.
6. Negative test unsubscribe nonexistent Subscriber ID.

## Test subscriptions:
1. Subscribe twice changing the event handler.
2. Subscribe just after changing it.

## Component must reconnect when server or network errors are encountered

## Infra test:
1- When redis goes down and then comes back up - client is able to connect

## Version related:
a. Insert a new configuration, version will be 1
b. Update Value for this config with version equal to 1 - new version will be 2.

## enableTLS set to true & enableTLS not integer:
Testing by creating component with ignoreErrors: true and then trying to use it, by trying to save, which should error out as state store never got configured successfully.

## Resiliency related:
1. Test if retries and backoff are respected.