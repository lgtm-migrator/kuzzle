/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2020 Kuzzle
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

import path from 'path';

import { murmurHash128 as murmur } from 'murmurhash-native';
import stringify from 'json-stable-stringify';
import { Koncorde } from 'koncorde';
import Bluebird from 'bluebird';
import segfaultHandler from 'segfault-handler';
import _ from 'lodash';

import kuzzleStateEnum from './kuzzleStateEnum';
import KuzzleEventEmitter from './event/kuzzleEventEmitter';
import EntryPoint from '../core/network/entryPoint';
import Funnel from '../api/funnel';
import PassportWrapper from '../core/auth/passportWrapper';
import PluginsManager from '../core/plugin/pluginsManager';
import Router from '../core/network/router';
import Statistics from '../core/statistics';
import { TokenManager } from '../core/auth/tokenManager';
import Validation from '../core/validation';
import Logger from './log';
import vault from './vault';
import DumpGenerator from './dumpGenerator';
import AsyncStore from '../util/asyncStore';
import { Mutex } from '../util/mutex';
import kerror from '../kerror';
import InternalIndexHandler from './internalIndexHandler';
import CacheEngine from '../core/cache/cacheEngine';
import StorageEngine from '../core/storage/storageEngine';
import SecurityModule from '../core/security';
import RealtimeModule from '../core/realtime';
import Cluster from '../cluster';
import { JSONObject } from './index';
import { InstallationConfig, ImportConfig, SupportConfig, StartOptions } from '../types/Kuzzle';
import { version } from '../../package.json';

const BACKEND_IMPORT_KEY = 'backend:init:import';

let _kuzzle = null;

Reflect.defineProperty(global, 'kuzzle', {
  configurable: true,
  enumerable: false,
  get () {
    if (_kuzzle === null) {
      throw new Error('Kuzzle instance not found. Did you try to use a live-only feature before starting your application?');
    }

    return _kuzzle;
  },
  set (value) {
    if (_kuzzle !== null) {
      throw new Error('Cannot build a Kuzzle instance: another one already exists');
    }

    _kuzzle = value;
  },
});

/**
 * @class Kuzzle
 * @extends EventEmitter
 */
export class Kuzzle extends KuzzleEventEmitter {
  private config: JSONObject;
  private _state: kuzzleStateEnum = kuzzleStateEnum.STARTING;
  private log: Logger;
  private rootPath: string;
  private internalIndex: InternalIndexHandler;
  private pluginsManager: PluginsManager;
  private tokenManager: TokenManager;
  private passport: PassportWrapper;
  private funnel: Funnel;
  private router: Router;
  private statistics: Statistics;
  private entryPoint: EntryPoint;
  private validation: Validation;
  private dumpGenerator: DumpGenerator;
  private vault: vault;
  private asyncStore: AsyncStore;
  private version: string;
  private importTypes: {
    [key: string]: (config: {
        toImport: ImportConfig,
        toSupport: SupportConfig}
      ) => Promise<void>;
  };
  private koncorde : Koncorde;
  private id : string;
  private secret : string;

  constructor (config: JSONObject) {
    super(
      config.plugins.common.maxConcurrentPipes,
      config.plugins.common.pipesBufferSize);

    global.kuzzle = this;

    this._state = kuzzleStateEnum.STARTING;

    this.config = config;

    this.log = new Logger();

    this.rootPath = path.resolve(path.join(__dirname, '../..'));

    // Internal index bootstrapper and accessor
    this.internalIndex = new InternalIndexHandler();

    this.pluginsManager = new PluginsManager();
    this.tokenManager = new TokenManager();
    this.passport = new PassportWrapper();

    // The funnel dispatches messages to API controllers
    this.funnel = new Funnel();

    // The router listens to client requests and pass them to the funnel
    this.router = new Router();

    // Statistics core component
    this.statistics = new Statistics();

    // Network entry point
    this.entryPoint = new EntryPoint();

    // Validation core component
    this.validation = new Validation();

    // Dump generator
    this.dumpGenerator = new DumpGenerator();

    // Vault component (will be initialized after bootstrap)
    this.vault = null;

    // AsyncLocalStorage wrapper
    this.asyncStore = new AsyncStore();

    // Kuzzle version
    this.version = version;

    // List of differents imports types and their associated method;
    this.importTypes = {
      fixtures: this._importFixtures.bind(this),
      mappings: this._importMappings.bind(this),
      permissions: this._importPermissions.bind(this),
      userMappings: this._importUserMappings.bind(this),
    };
  }

  /**
   * Initializes all the needed components of Kuzzle.
   *
   * @param {Application} - Application instance
   * @param {Object} - Additional options (import, installations, plugins, secretsFile, support, vaultKey)
   *
   * @this {Kuzzle}
   */
  async start (application: any, options: StartOptions = { import: {} }) {
    this.registerSignalHandlers();

    try {
      this.log.info(`[ℹ] Starting Kuzzle ${this.version} ...`);
      await super.pipe('kuzzle:state:start');
      

      // Koncorde realtime engine
      this.koncorde = new Koncorde({
        maxConditions: this.config.limits.subscriptionConditionsCount,
        regExpEngine: this.config.realtime.pcreSupport ? 'js' : 're2',
        seed: this.config.internal.hash.seed
      });

      await (new CacheEngine()).init();
      await (new StorageEngine()).init();
      await (new RealtimeModule()).init();
      await this.internalIndex.init();

      await (new SecurityModule()).init();

      this.id = await (new Cluster()).init();

      // Secret used to generate JWTs
      this.secret = await this.internalIndex.getSecret();

      this.vault = vault.load(options.vaultKey, options.secretsFile);

      await this.validation.init();

      await this.tokenManager.init();

      await this.funnel.init();

      this.statistics.init();

      await this.validation.curateSpecification();

      // must be initialized before plugins to allow API requests from plugins
      // before opening connections to external users
      await this.entryPoint.init();

      this.pluginsManager.application = application;
      await this.pluginsManager.init(options.plugins);
      this.log.info(`[✔] Successfully loaded ${this.pluginsManager.plugins.length} plugins: ${this.pluginsManager.plugins.map(p => p.name).join(', ')}`);

      // Authentification plugins must be loaded before users import to avoid
      // credentials related error which would prevent Kuzzle from starting
      await this.import(options.import, options.support);

      await super.ask('core:security:verify');

      this.router.init();

      this.log.info('[✔] Core components loaded');

      await this.install(options.installations);

      // @deprecated
      await super.pipe('kuzzle:start');

      await super.pipe('kuzzle:state:live');

      await this.entryPoint.startListening();

      await super.pipe('kuzzle:state:ready');

      this.log.info(`[✔] Kuzzle ${this.version} is ready (node name: ${this.id})`);

      // @deprecated
      super.emit('core:kuzzleStart', 'Kuzzle is ready to accept requests');

      this._state = kuzzleStateEnum.RUNNING;
    }
    catch(error) {
      this.log.error(`[X] Cannot start Kuzzle ${this.version}: ${error.message}`);

      throw error;
    }
  }

  /**
   * Gracefully exits after processing remaining requests
   *
   * @returns {Promise}
   */
  async shutdown (): Promise<void> {
    this._state = kuzzleStateEnum.SHUTTING_DOWN;

    this.log.info('Initiating shutdown...');
    await super.pipe('kuzzle:shutdown');

    // @deprecated
    super.emit('core:shutdown');

    // Ask the network layer to stop accepting new request
    this.entryPoint.dispatch('shutdown');

    while (this.funnel.remainingRequests !== 0) {
      this.log.info(`[shutdown] Waiting: ${this.funnel.remainingRequests} remaining requests`);
      await Bluebird.delay(1000);
    }

    this.log.info('Halted.');

    process.exit(0);
  }

  /**
   * Execute multiple handlers only once on any given environment
   *
   * @param {Array<{ id: string, handler: () => void, description?: string }>} installations - Array of unique methods to execute
   *
   * @returns {Promise<void>}
   */
  async install (installations: Array<InstallationConfig>): Promise<void> {
    if (! installations || ! installations.length) {
      return;
    }

    const mutex = new Mutex('backend:installations');
    await mutex.lock();

    try {
      for (const installation of installations) {
        const isAlreadyInstalled = await super.ask(
          'core:storage:private:document:exist',
          'kuzzle',
          'installations',
          installation.id );

        if (! isAlreadyInstalled) {
          try {
            await installation.handler();
          }
          catch (error) {
            throw kerror.get(
              'plugin',
              'runtime',
              'unexpected_installation_error',
              installation.id, error);
          }

          await super.ask(
            'core:storage:private:document:create',
            'kuzzle',
            'installations',
            {
              description: installation.description,
              handler: installation.handler.toString(),
              installedAt: Date.now() },
            { id: installation.id });

          this.log.info(`[✔] Install code "${installation.id}" successfully executed`);
        }
      }
    }
    finally {
      await mutex.unlock();
    }
  }

  async _importUserMappings(config: {
    toImport: ImportConfig,
    toSupport: SupportConfig
  }): Promise<void> {
    const toImport = config.toImport;

    if (! _.isEmpty(toImport.userMappings)) {
      await this.internalIndex.updateMapping('users', toImport.userMappings);
      await this.internalIndex.refreshCollection('users');
      this.log.info('[✔] User mappings import successful');
    }
  }

  async _importMappings(config: {
    toImport: ImportConfig,
    toSupport: SupportConfig
  }): Promise<void> {
    const toImport = config.toImport;
    const toSupport = config.toSupport;

    if (! _.isEmpty(toSupport.mappings) && ! _.isEmpty(toImport.mappings)) {
      throw kerror.get(
        'plugin',
        'runtime',
        'incompatible',
        '_support.mappings',
        'import.mappings');
    }
    else if (! _.isEmpty(toSupport.mappings)) {
      await super.ask(
        'core:storage:public:mappings:import',
        toSupport.mappings,
        {
          rawMappings: true,
          refresh: true
        });
      this.log.info('[✔] Mappings import successful');
    }
    else if (! _.isEmpty(toImport.mappings)) {
      await super.ask('core:storage:public:mappings:import',
        toImport.mappings,
        { refresh: true });
      this.log.info('[✔] Mappings import successful');
    }
  }

  async _importFixtures(config: {
    toImport: ImportConfig,
    toSupport: SupportConfig
  }): Promise<void> {
    const toSupport = config.toSupport;
    
    if (! _.isEmpty(toSupport.fixtures)) {
      await super.ask('core:storage:public:document:import', toSupport.fixtures);
      this.log.info('[✔] Fixtures import successful');
    }
  }

  async _importPermissions(config: {
    toImport: ImportConfig,
    toSupport: SupportConfig
  }): Promise<void> {
    const toImport = config.toImport;
    const toSupport = config.toSupport;

    const isPermissionsToImport = !(
      _.isEmpty(toImport.profiles)
      && _.isEmpty(toImport.roles)
      && _.isEmpty(toImport.users)
    );
    const isPermissionsToSupport = toSupport.securities
      && !(
        _.isEmpty(toSupport.securities.profiles)
        && _.isEmpty(toSupport.securities.roles)
        && _.isEmpty(toSupport.securities.users)
      );
    if (isPermissionsToSupport && isPermissionsToImport) {
      throw kerror.get(
        'plugin',
        'runtime',
        'incompatible',
        '_support.securities',
        'import profiles roles or users');
    }
    else if (isPermissionsToSupport) {
      await super.ask('core:security:load', toSupport.securities,
        {
          force: true,
          refresh: 'wait_for'
        });
      this.log.info('[✔] Securities import successful');
    }
    else if (isPermissionsToImport) {
      await super.ask('core:security:load',
        {
          profiles: toImport.profiles,
          roles: toImport.roles,
          users: toImport.users,
        },
        {
          onExistingUsers: toImport.onExistingUsers,
          onExistingUsersWarning: true,
          refresh: 'wait_for',
        });
      this.log.info('[✔] Permissions import successful');
    }
  }

  /**
   * Load into the app several imports
   *
   * @param {Object} toImport - Contains `mappings`, `onExistingUsers`, `profiles`, `roles`, `userMappings`, `users`
   * @param {Object} toSupport - Contains `fixtures`, `mappings`, `securities` (`profiles`, `roles`, `users`)
   *
   * @returns {Promise<void>}
   */
  async import (
    toImport: ImportConfig = {},
    toSupport: SupportConfig = {}
  ): Promise<void> {
    const lockedMutex = [];

    try {
      for (const [type, importMethod] of Object.entries(this.importTypes)) {
        const mutex = new Mutex(`backend:import:${type}`, { timeout: 0 });
        if (! await super.ask('core:cache:internal:get', `${BACKEND_IMPORT_KEY}:${type}`)
          && await mutex.lock()
        ) {
          lockedMutex.push(mutex);
          await importMethod({ toImport, toSupport });
          await super.ask('core:cache:internal:store', `${BACKEND_IMPORT_KEY}:${type}`, 1);
        }
      }

      
      this.log.info('[✔] Waiting for imports to be finished');
      /**
       * Check if every import has been done, if one of them is not finished yet, wait for it
       */
      let initialized = false;
      while (! initialized) {
        initialized = true;

        for (const type of Object.keys(this.importTypes)) {
          if (! await super.ask('core:cache:internal:get', `${BACKEND_IMPORT_KEY}:${type}`)) {
            initialized = false;
            break;
          }
        }

        if (! initialized) {
          await Bluebird.delay(1000);
        }
      }

      this.log.info('[✔] Import successful');
    } finally {
      await Promise.all(lockedMutex.map(mutex => mutex.unlock()));
    }
  }

  dump (suffix) {
    return this.dumpGenerator.dump(suffix);
  }

  hash (input: any) {
    let inString;

    switch (typeof input) {
      case 'string':
      case 'number':
      case 'boolean':
        inString = input;
        break;
      default:
        inString = stringify(input);
    }

    return murmur(Buffer.from(inString), 'hex', this.config.internal.hash.seed);
  }

  get state () {
    return this._state;
  }

  set state (value) {
    this._state = value;
    super.emit('kuzzle:state:change', value);
  }

  /**
   * Register handlers and do a kuzzle dump for:
   * - system signals
   * - unhandled-rejection
   * - uncaught-exception
   */
  registerSignalHandlers () {
    process.removeAllListeners('unhandledRejection');
    process.on('unhandledRejection', (reason, promise) => {
      if (reason !== undefined) {
        if (reason instanceof Error) {
          this.log.error(`ERROR: unhandledRejection: ${reason.message}. Reason: ${reason.stack}`);
        }
        else {
          this.log.error(`ERROR: unhandledRejection: ${reason}`);
        }
      }
      else {
        this.log.error(`ERROR: unhandledRejection: ${promise}`);
      }

      // Crashing on an unhandled rejection is a good idea during development
      // as it helps spotting code errors. And according to the warning messages,
      // this is what Node.js will do automatically in future versions anyway.
      if (global.NODE_ENV === 'development') {
        this.log.error('Kuzzle caught an unhandled rejected promise and will shutdown.');
        this.log.error('This behavior is only triggered if global.NODE_ENV is set to "development"');

        throw reason;
      }
    });

    process.removeAllListeners('uncaughtException');
    process.on('uncaughtException', err => {
      this.log.error(`ERROR: uncaughtException: ${err.message}\n${err.stack}`);
      this.dumpAndExit('uncaught-exception');
    });

    // abnormal termination signals => generate a core dump
    for (const signal of ['SIGQUIT', 'SIGABRT']) {
      process.removeAllListeners(signal);
      process.on(signal, () => {
        this.log.error(`ERROR: Caught signal: ${signal}`);
        this.dumpAndExit('signal-'.concat(signal.toLowerCase()));
      });
    }

    // signal SIGTRAP is used to generate a kuzzle dump without stopping it
    process.removeAllListeners('SIGTRAP');
    process.on('SIGTRAP', () => {
      this.log.error('Caught signal SIGTRAP => generating a core dump');
      this.dump('signal-sigtrap');
    });

    // gracefully exits on normal termination
    for (const signal of ['SIGINT', 'SIGTERM']) {
      process.removeAllListeners(signal);
      process.on(signal, () => {
        this.log.info(`Caught signal ${signal} => gracefully exit`);
        this.shutdown();
      });
    }

    segfaultHandler.registerHandler();
  }

  async dumpAndExit (suffix) {
    if (this.config.dump.enabled) {
      try {
        await this.dump(suffix);
      }
      catch(error) {
        // this catch is just there to prevent unhandled rejections, there is
        // nothing to do with that error
      }
    }

    await this.shutdown();
  }
}
