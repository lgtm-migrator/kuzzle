---
type: page
code: false
title: Realtime
description: Realtime events list
order: 100
---

# Realtime events

This page contains all non-API events related to realtime actions in Kuzzle.

---

## core:hotelClerk:addSubscription

This event is deprecated and will be removed in the next major version of Kuzzle.

Use `core:realtime:user:subscribe:after` instead.

<DeprecatedBadge version="2.5.0">

| Arguments      | Type              | Description                                       |
| -------------- | ----------------- | ------------------------------------------------- |
| `subscription` | <pre>object</pre> | Contains information about the added subscription |

Triggered whenever a [subscription](/core/2/api/controllers/realtime/subscribe) is added.

### subscription

The provided `subscription` object has the following properties:

| Properties     | Type               | Description                                                                                                    |
| -------------- | ------------------ | -------------------------------------------------------------------------------------------------------------- |
| `roomId`       | <pre>string</pre>  | Room unique identifier                                                                                         |
| `connectionId` | <pre>integer</pre> | [ClientConnection](/core/2/guides/write-protocols/context/clientconnection) unique identifier                  |
| `index`        | <pre>string</pre>  | Index                                                                                                          |
| `collection`   | <pre>string</pre>  | Collection                                                                                                     |
| `filters`      | <pre>object</pre>  | Filters in [Koncorde's normalized format](https://github.com/kuzzleio/koncorde/wiki/Filter-Unique-Identifiers) |

</DeprecatedBadge>

---

## core:hotelClerk:removeRoomForCustomer

This event is deprecated and will be removed in the next major version of Kuzzle.

Use `core:realtime:user:unsubscribe:after` instead.

<DeprecatedBadge version="2.5.0">

| Arguments        | Type              | Description                                                                     |
| ---------------- | ----------------- | ------------------------------------------------------------------------------- |
| `RequestContest` | <pre>object</pre> | [requestContext](/core/2/guides/write-protocols/context/requestcontext/) object |
| `room`           | <pre>object</pre> | Joined room information in Koncorde format                                      |

Triggered whenever a user is removed from a room. 

### room

The provided `room` object has the following properties:

| Properties   | Type              | Description            |
| ------------ | ----------------- | ---------------------- |
| `id`         | <pre>string</pre> | Room unique identifier |
| `index`      | <pre>string</pre> | Index                  |
| `collection` | <pre>string</pre> | Collection             |

</DeprecatedBadge>

---

## core:realtime:room:create:after

<SinceBadge version="2.5.0"/>

Triggered whenever a new realtime room is subscribed (NOT triggered if a user subscribes to an existing room).

:::info
Pipes cannot listen to this event, only hooks can.
:::

| Arguments | Type              | Description             |
| --------- | ----------------- | ----------------------- |
| `room`    | <pre>object</pre> | Joined room information |

### room

The provided `room` object has the following properties:

| Properties   | Type              | Description                    |
| ------------ | ----------------- | ------------------------------ |
| `index`      | <pre>string</pre> | Index name                     |
| `collection` | <pre>string</pre> | Collection name                |
| `roomId`     | <pre>string</pre> | The new room unique identifier |

---

## core:realtime:room:remove:before

<SinceBadge version="2.5.0"/>

Triggered whenever a realtime room is deleted, which happens when the last subscriber leaves it.

:::info
Pipes cannot listen to this event, only hooks can.
:::

| Arguments | Type              | Description            |
| --------- | ----------------- | ---------------------- |
| `roomId`  | <pre>string</pre> | Room unique identifier |


---

## core:realtime:user:subscribe:after

Triggered whenever a user makes a new [subscription](/core/2/api/controllers/realtime/subscribe).

:::info
Pipes cannot listen to this event, only hooks can.
:::

<SinceBadge version="2.5.0"/>

| Arguments      | Type              | Description                                       |
| -------------- | ----------------- | ------------------------------------------------- |
| `subscription` | <pre>object</pre> | Contains information about the added subscription |


### subscription

The provided `subscription` object has the following properties:

| Properties     | Type               | Description                                                                                                |
| -------------- | ------------------ | ---------------------------------------------------------------------------------------------------------- |
| `roomId`       | <pre>string</pre>  | Room unique identifier                                                                                     |
| `connectionId` | <pre>integer</pre> | [ClientConnection](/core/2/guides/write-protocols/context/clientconnection) unique identifier              |
| `index`        | <pre>string</pre>  | Index                                                                                                      |
| `collection`   | <pre>string</pre>  | Collection                                                                                                 |
| `filters`      | <pre>object</pre>  | Filters in [Koncorde's normalized format](https://www.npmjs.com/package/koncorde#filter-unique-identifier) |
| `kuid`         | <pre>string</pre>  | ID of the user <SinceBadge version="2.14.1" />                                                             |

---

## core:realtime:user:unsubscribe:after

Triggered whenever a user leaves a room.

:::info
Pipes cannot listen to this event, only hooks can.
:::

<SinceBadge version="2.5.0"/>

| Arguments        | Type              | Description                                                                                                          |
| ---------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| `RequestContest` | <pre>object</pre> | [RequestContext](/core/2/guides/write-protocols/context/requestcontext/) object <DeprecatedBadge version="2.14.1" /> |
| `room`           | <pre>object</pre> | Joined room information in Koncorde format <DeprecatedBadge version="2.14.1" />                                      |
| `subscription`   | <pre>object</pre> | Contains information about the removed subscription <SinceBadge version="2.14.1" />                                  |

### room

The provided `room` object has the following properties:

| Properties   | Type              | Description            |
| ------------ | ----------------- | ---------------------- |
| `id`         | <pre>string</pre> | Room unique identifier |
| `index`      | <pre>string</pre> | Index                  |
| `collection` | <pre>string</pre> | Collection             |

### subscription

The provided `subscription` object has the following properties:

| Properties     | Type               | Description                                                                                   |
| -------------- | ------------------ | --------------------------------------------------------------------------------------------- |
| `roomId`       | <pre>string</pre>  | Room unique identifier                                                                        |
| `connectionId` | <pre>integer</pre> | [ClientConnection](/core/2/guides/write-protocols/context/clientconnection) unique identifier |
| `index`        | <pre>string</pre>  | Index                                                                                         |
| `collection`   | <pre>string</pre>  | Collection                                                                                    |
| `kuid`         | <pre>string</pre>  | ID of the user                                                                                |

---

## core:realtime:notification:dispatch:before

<SinceBadge version="2.19.1"/>

| Arguments             | Type              | Description                                 |
| --------------------- | ----------------- | ------------------------------------------- |
| `notificationContext` | <pre>object</pre> | Contains information about the notification |

### notificationContext

| Arguments      | Type                                                                    | Description                                                                                   |
| -------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `notification` | <pre><a href="/core/2/api/payloads/notifications">Notifications</a></pre> | The normalized real-time notification                                                         |
| `channels`     | <pre>string[]</pre>                                                     | List of Subscribers channels to notify                                                        |
| `connectionId` | <pre>integer</pre>                                                      | [ClientConnection](/core/2/guides/write-protocols/context/clientconnection) unique identifier |

---

## notify:dispatch

<DeprecatedBadge version="2.19.1">

This event is deprecated and will be removed in the next major version of Kuzzle.

Use `core:realtime:notification:dispatch:before` instead.

| Arguments | Type                                                                    | Description                           |
| --------- | ----------------------------------------------------------------------- | ------------------------------------- |
| `message` | <pre><a href=/core/2/api/payloads/notifications>Notifications</a></pre> | The normalized real-time notification |

Triggered whenever a real-time notification is about to be sent.

</DeprecatedBadge>

---

## notify:document

| Arguments | Type                                                                    | Description                           |
| --------- | ----------------------------------------------------------------------- | ------------------------------------- |
| `message` | <pre><a href=/core/2/api/payloads/notifications>Notifications</a></pre> | The normalized real-time notification |

Triggered whenever a real-time document notification is about to be sent.

---

## notify:server

| Arguments | Type                                                                    | Description                           |
| --------- | ----------------------------------------------------------------------- | ------------------------------------- |
| `message` | <pre><a href=/core/2/api/payloads/notifications>Notifications</a></pre> | The normalized real-time notification |

Triggered whenever a real-time server notification is about to be sent.

---

## notify:user

| Arguments | Type                                                                    | Description                           |
| --------- | ----------------------------------------------------------------------- | ------------------------------------- |
| `message` | <pre><a href=/core/2/api/payloads/notifications>Notifications</a></pre> | The normalized real-time notification |

Triggered whenever a real-time user notification is about to be sent.

---

## room:new

This event is deprecated and will be removed in the next major version of Kuzzle.

Use `core:realtime:room:create:after` instead.

<DeprecatedBadge version="2.5.0">


| Arguments | Type              | Description             |
| --------- | ----------------- | ----------------------- |
| `room`    | <pre>object</pre> | Joined room information |

Triggered whenever a new [subscription](/core/2/api/controllers/realtime/subscribe) is created.

:::info
Pipes cannot listen to this event, only hooks can.
:::

### room

The provided `room` object has the following properties:

| Properties   | Type              | Description                    |
| ------------ | ----------------- | ------------------------------ |
| `index`      | <pre>string</pre> | Index name                     |
| `collection` | <pre>string</pre> | Collection name                |
| `roomId`     | <pre>string</pre> | The new room unique identifier |

</DeprecatedBadge>

---

## room:remove

This event is deprecated and will be removed in the next major version of Kuzzle.

Use `core:realtime:room:remove:before` instead.

<DeprecatedBadge version="2.5.0">

| Arguments | Type              | Description            |
| --------- | ----------------- | ---------------------- |
| `roomId`  | <pre>string</pre> | Room unique identifier |

Triggered whenever a real-time subscription is cancelled.

:::info
Pipes cannot listen to this event, only hooks can.
:::

</DeprecatedBadge>

---
