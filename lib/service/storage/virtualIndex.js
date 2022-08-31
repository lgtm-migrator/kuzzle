"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.VirtualIndex = void 0;
const crypto_1 = __importDefault(require("crypto"));
const service_1 = require("../service");
class VirtualIndex extends service_1.Service {
    constructor() {
        super("VirtualIndex", global.kuzzle.config.services.storageEngine);
        this.softTenant = new Map(); //Key : virtual index, value : real index //TODO rename!
    }
    async initWithClient(clientAdapter) {
        this.clientAdapter = clientAdapter;
        await this.buildCollection();
        await this.initVirtualTenantList();
        await global.kuzzle.ask("cluster:event:on", VirtualIndex.createEvent, (info) => this.editSoftTenantMap(info));
    }
    editSoftTenantMap(notification) {
        if (notification.action === "create" || notification.action === "update") {
            const source = notification.result._source;
            this.softTenant.set(source.virtual, source.real);
        }
    }
    getRealIndex(name) {
        if (this.softTenant.has(name)) {
            return this.softTenant.get(name);
        }
        return name;
    }
    isVirtual(name) {
        return this.softTenant.has(name);
    }
    getId(index, id) {
        if (this.isVirtual(index)) {
            return index + id;
        }
        return id;
    }
    randomString(size = 20) {
        return crypto_1.default.randomBytes(size).toString("base64").slice(0, size);
    }
    getVirtualId(index, id) {
        if (this.isVirtual(index) && id.startsWith(index)) {
            return id.substring(index.length, id.length);
        }
        return id;
    }
    async createVirtualIndex(virtualIndex, index) {
        //TODO : cluster //TODO : throw exception if "index" is virtual
        await global.kuzzle.ask("cluster:event:broadcast", VirtualIndex.createEvent, { real: index, virtual: virtualIndex });
        //this.editSoftTenantMap({ real: index, virtual: virtualIndex });
        this.softTenant.set(virtualIndex, index);
        await global.kuzzle.ask("core:storage:private:document:create", "virtualindexes", "list", { real: index, virtual: virtualIndex }, { id: index + virtualIndex });
    }
    async removeVirtualIndex(index) {
        //TODO : persistance
        const realIndex = this.softTenant.get(index);
        this.softTenant.delete(index);
        const id = realIndex + index;
        await global.kuzzle.ask("core:storage:private:document:delete", "virtualindexes", "list", id);
    }
    async initVirtualTenantList() {
        //TODO : from database
        if (this.softTenant.size === 0) {
            this.softTenant = new Map();
            //this.softTenant.set("virtual-index", "hard-index"); //TODO : micro-controller
            //this.softTenant.set("virtual-index-2", "index2"); //TODO : remove
            //this.softTenant.set("virtual-index-3", "index2"); //TODO : remove
            let from = 0;
            let total = Number.MAX_VALUE;
            do {
                const list = await global.kuzzle.ask("core:storage:private:document:search", "virtualindexes", "list", { from: from, size: 100 });
                total = list.total;
                for (const hit of list.hits) {
                    this.softTenant.set(hit._source.virtual, hit._source.real);
                }
                from += 100;
            } while (from < total);
        }
    }
    async buildCollection() {
        try {
            await this.clientAdapter.createIndex("virtualindexes"); //Replace with kuzzle.ask()
        }
        catch (e) {
            /* already created */
        }
        try {
            await this.clientAdapter.createCollection("virtualindexes", "list", {
                //Replace with kuzzle.ask()
                mappings: {
                    _meta: undefined,
                    dynamic: "strict",
                    properties: {
                        real: { type: "text" },
                        virtual: { type: "text" },
                    },
                },
            });
        }
        catch (e) {
            /* already created */
        }
    }
}
exports.VirtualIndex = VirtualIndex;
VirtualIndex.createEvent = "virtualindex:create";
//# sourceMappingURL=virtualIndex.js.map