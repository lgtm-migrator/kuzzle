/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2022 Kuzzle
 * mailto: support AT kuzzle.io
 * website: http://kuzzle.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Most of the functions exposed in this file should be viewed as
// critical section of code.

import assert from "assert";

import EventEmitter from "eventemitter3";
import Bluebird from "bluebird";
import createDebug from "debug";
import * as uuid from "uuid";

import Promback from "../../util/promback";
import memoize from "../../util/memoize";
import PipeRunner from "./pipeRunner";
import * as kerror from "../../kerror";
import { EventDefinition, PipeEventHandler } from "../../types/EventHandler";

const debug = createDebug("kuzzle:events");

/**
 * Type used with `ask<T>()` and `onAsk<T>()` methods.
 */
export type AskEventDefinition = {
  name: string;

  args: any[];

  result: any;
}

/**
 * Type used with `onCall<T>()` and `call<T>()` methods.
 */
export type SyncEventDefinition = {
  name: string;

  args: any[];

  result: any;
}

export type AskEventHandler<TAskEventDefinition extends AskEventDefinition = AskEventDefinition
> = (...args: TAskEventDefinition["args"]) => Promise<TAskEventDefinition["result"]>

export type SyncEventHandler<TSyncEventDefinition extends SyncEventDefinition = SyncEventDefinition
> = (...args: TSyncEventDefinition["args"]) => Promise<TSyncEventDefinition["result"]>

class PluginPipeDefinition {
  public event: string;
  public handler: PipeEventHandler;
  public pipeId: string;

  constructor(event: string, handler: PipeEventHandler, pipeId: string = null) {
    this.event = event;
    this.handler = handler;
    this.pipeId = pipeId || uuid.v4();
  }
}

export class KuzzleEventEmitter extends EventEmitter {
  public superEmit: EventEmitter["emit"];

  private pipeRunner: PipeRunner;

  /**
   * Map of plugin pipe handler functions by event
   */
  private pluginPipes: Map<string, PipeEventHandler[]> = new Map();
  /**
   * Map of plugin pipe definitions by pipeId
  */
  private pluginPipeDefinitions: Map<string, PluginPipeDefinition> = new Map();
  private corePipes: Map<string, PipeEventHandler[]> = new Map();
  private coreAnswerers: Map<string, AskEventHandler> = new Map();
  private coreSyncedAnswerers: Map<string, SyncEventHandler> = new Map();

  constructor(maxConcurrentPipes: number, pipesBufferSize: number) {
    super();

    this.superEmit = super.emit;
    this.pipeRunner = new PipeRunner(maxConcurrentPipes, pipesBufferSize);
  }

  /**
   * Registers a core method on a pipe
   *
   * Note: core methods cannot listen to wildcarded events, only exact matching
   * works here.
   */
  onPipe<
  TEventDefinition extends EventDefinition = EventDefinition
>(event: TEventDefinition["name"], handler: PipeEventHandler<TEventDefinition>) {
    assert(
      typeof handler === "function",
      `Cannot listen to pipe event ${event}: "${handler}" is not a function`
    );

    if (!this.corePipes.has(event)) {
      this.corePipes.set(event, []);
    }

    this.corePipes.get(event).push(handler);
  }

  /**
   * Registers a core 'ask' event answerer
   * There can only be 0 or 1 answerer per ask event.
   */
  onAsk<TAskEventDefinition extends AskEventDefinition = AskEventDefinition
  >(event: TAskEventDefinition["name"], handler: AskEventHandler<TAskEventDefinition>) {
    assert(
      typeof handler === "function",
      `Cannot listen to ask event "${event}": "${handler}" is not a function`
    );
    assert(
      !this.coreAnswerers.has(event),
      `Cannot add a listener to the ask event "${event}": event has already an answerer`
    );

    this.coreAnswerers.set(event, handler);
  }

  /**
   * Registers a core 'callback' answerer
   * There can only be 0 or 1 answerer per callback event.
   */
  onCall<TSyncEventDefinition extends SyncEventDefinition = SyncEventDefinition
  >(event: TSyncEventDefinition["name"], handler: SyncEventHandler<TSyncEventDefinition>) {
    assert(
      typeof handler === "function",
      `Cannot register callback for event "${event}": "${handler}" is not a function`
    );
    assert(
      !this.coreSyncedAnswerers.has(event),
      `Cannot register callback for event "${event}": a callback has already been registered`
    );

    this.coreSyncedAnswerers.set(event, handler);
  }

  /**
   * Emits an event and all its wildcarded versions
   *
   * @warning Critical section of code
   */
  emit(event: string | Symbol, ...data: any[]): boolean {
    const events = getWildcardEvents(event);
    debug('Triggering event "%s" with data: %o', event, data);

    for (let i = 0; i < events.length; i++) {
      super.emit(events[i], data);
    }

    return true;
  }

  /**
   * Emits a pipe event, which triggers the following, in that order:
   * 1. Plugin pipes are invoked one after another (waterfall). Each plugin must
   *    resolve the pipe (with a callback or a promise) with a similar payload
   *    than the one received
   * 2. Core pipes are invoked in parallel. They are awaited for (promises-only)
   *    but their responses are neither evaluated nor used
   * 3. Hooks are invoked in parallel. They are not awaited.
   *
   * Accepts a callback argument (to be used by pipes invoked before the funnel
   * overload-protection mechanism). If a callback is provided, this method
   * doesn't return a promise.
   *
   * @warning Critical section of code
   */
  pipe<
  TEventDefinition extends EventDefinition = EventDefinition
>(event: string, ...args: TEventDefinition["args"]): Promise<TEventDefinition["args"][0]> {
    debug('Triggering pipe "%s" with args: %o', event, args);

    let callback = null;

    // safe: a pipe's payload can never contain functions
    if (
      args.length > 0 &&
      typeof args[args.length - 1] === "function"
    ) {
      callback = args.pop();
    }

    const events = getWildcardEvents(event);
    const funcs = [];

    for (let i = 0; i < events.length; i++) {
      const targets = this.pluginPipes.get(events[i]);

      if (targets) {
        targets.forEach((t) => funcs.push(t));
      }
    }

    // Create a context for the emitPluginPipe callback
    const promback = new Promback(callback);
    const callbackContext = {
      events,
      instance: this,
      promback,
      targetEvent: event,
    };

    if (funcs.length === 0) {
      pipeCallback.call(callbackContext, null, ...args);
    } else {
      this.pipeRunner.run(funcs, args, pipeCallback, callbackContext);
    }

    return promback.deferred;
  }

  /**
   * Emits an "ask" event to get information about the provided payload
   */
  async ask<TAskEventDefinition extends AskEventDefinition = AskEventDefinition
  >(event: TAskEventDefinition["name"], ...args: TAskEventDefinition["args"]): Promise<TAskEventDefinition["result"]> {
    debug('Triggering ask "%s" with args: %o', event, args);

    const handler = this.coreAnswerers.get(event);

    if (!handler) {
      throw kerror.get(
        "core",
        "fatal",
        "assertion_failed",
        `the requested ask event '${event}' doesn't have an answerer`
      );
    }

    const response = await handler(...args);

    getWildcardEvents(event).forEach((ev) =>
      super.emit(ev, {
        args: args,
        response,
      })
    );

    return response;
  }

  /**
   * Calls a callback to get information about the provided payload
   */
  call<TSyncEventDefinition extends SyncEventDefinition = SyncEventDefinition
  >(event: TSyncEventDefinition["name"], ...args: TSyncEventDefinition["args"]) {
    debug('Triggering callback "%s" with args: %o', event, args);

    const fn = this.coreSyncedAnswerers.get(event);

    if (!fn) {
      throw kerror.get(
        "core",
        "fatal",
        "assertion_failed",
        `the requested callback event '${event}' doesn't have an answerer`
      );
    }

    const response = fn(...args);

    getWildcardEvents(event).forEach((ev) =>
      super.emit(ev, {
        args,
        response,
      })
    );

    return response;
  }

  /**
   * Registers a plugin hook.
   * Catch any error in the handler and emit the hook:onError event.
   */
  registerPluginHook(pluginName, event, fn) {
    this.on(event, (...args) => {
      try {
        const ret = fn(...args, event);

        if (typeof ret === "object" && typeof ret.catch === "function") {
          ret.catch((error) => {
            if (event !== "hook:onError") {
              this.emit("hook:onError", { error, event, pluginName });
            } else {
              this.emit("plugin:hook:loop-error", { error, pluginName });
            }
          });
        }
      } catch (error) {
        if (event !== "hook:onError") {
          this.emit("hook:onError", { error, event, pluginName });
        } else {
          this.emit("plugin:hook:loop-error", { error, pluginName });
        }
      }
    });
  }

  registerPluginPipe(event, handler) {
    if (!this.pluginPipes.has(event)) {
      this.pluginPipes.set(event, []);
    }

    this.pluginPipes.get(event).push(handler);

    const definition = new PluginPipeDefinition(event, handler);

    this.pluginPipeDefinitions.set(definition.pipeId, definition);

    return definition.pipeId;
  }

  unregisterPluginPipe(pipeId) {
    const definition = this.pluginPipeDefinitions.get(pipeId);

    if (!definition) {
      throw kerror.get("plugin", "runtime", "unknown_pipe", pipeId);
    }

    const handlers = this.pluginPipes.get(definition.event);
    handlers.splice(handlers.indexOf(definition.handler), 1);
    if (handlers.length > 0) {
      this.pluginPipes.set(definition.event, handlers);
    } else {
      this.pluginPipes.delete(definition.event);
    }

    this.pluginPipeDefinitions.delete(pipeId);
  }

  /**
   * Checks if an ask event has an answerer
   *
   * @param  {string}  event
   * @return {Boolean}       [description]
   */
  hasAskAnswerer(event) {
    return this.coreAnswerers.has(event);
  }
}

/**
 * We declare the callback used by Kuzzle.pipe one time instead
 * of redeclaring a closure each time we want to run the pipes.
 *
 * The context of this callback must be bound to this following object:
 * { instance: (kuzzle instance), promback, events }
 *
 * @warning Critical section of code
 */
async function pipeCallback(error, ...updated) {
  /* eslint-disable no-invalid-this */
  if (error) {
    this.promback.reject(error);
    return;
  }

  const corePipes = this.instance.corePipes.get(this.targetEvent);

  if (corePipes) {
    await Bluebird.map(corePipes, (fn) => fn(...updated));
  }

  for (let i = 0; i < this.events.length; i++) {
    this.instance.superEmit(this.events[i], ...updated);
  }

  this.promback.resolve(updated[0]);
  /* eslint-enable no-invalid-this */
}

/**
 * For a specific event, returns the event and all its wildcarded versions
 * @example
 *  getWildcardEvents('data:create') // return ['data:create', 'data:*']
 *  getWildcardEvents('data:beforeCreate') // return ['data:beforeCreate',
 *                                         //         'data:*', 'data:before*']
 *
 * @warning Critical section of code
 *
 * @param {String} event
 * @returns {Array<String>} wildcard events
 */
const getWildcardEvents = memoize((event) => {
  const events = [event];
  const delimIndex = event.lastIndexOf(":");

  if (delimIndex === -1) {
    return events;
  }

  const scope = event.slice(0, delimIndex);
  const name = event.slice(delimIndex + 1);

  ["before", "after"].forEach((prefix) => {
    if (name.startsWith(prefix)) {
      events.push(`${scope}:${prefix}*`);
    }
  });

  events.push(`${scope}:*`);

  return events;
});

module.exports = KuzzleEventEmitter;
