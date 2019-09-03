import * as Redis from 'ioredis';

type SchemaType = 'string' | 'number' | 'Date';
interface SchemaEntry<T> {
    type: SchemaType;
    index?: boolean;
}

type Schema<T> = { [key: string]: SchemaEntry<T> };

type Filter<T> = {
    [key: string]: {
        orderKey: string;
        condition: (elem: T) => void;
    };
};

export class RedisRichStructure<T extends { id?: number | string }> {
    private dateKeys: string[];
    constructor(
        private redis: Redis.Redis,
        private collectionName: string,
        private schema: Schema<T>,
        private filters: Filter<T>,
        private autoId: boolean
    ) {
        this.dateKeys = Object.keys(this.schema).filter(
            key => this.schema[key].type === 'Date'
        );
    }

    private getCntKey() {
        return `idcnt::${this.collectionName}`;
    }

    private getKey(id?: number | string) {
        return `${this.collectionName}:${id}`;
    }

    private getIndexKey(key: string) {
        return `index::${key}`;
    }

    private getStringIndexKey(key: string, value: string) {
        return `index::${key}:${value}`;
    }

    private getFilterKey(filterName: string) {
        return `filter::${filterName}`;
    }

    private async insertIndex(_elems: T[]) {
        for (let key of Object.keys(this.schema)) {
            if (!this.schema[key].index) continue;
            const elems = _elems.filter(elem => elem[key] !== undefined);
            if (!elems.length) continue;
            if (this.schema[key].type === 'string') {
                for (let elem of elems) {
                    const value: string = elem[key];
                    await this.redis.sadd(
                        this.getStringIndexKey(key, value),
                        elem.id
                    );
                }
            } else if (this.schema[key].type === 'number') {
                const args: string[] = [];
                for (let elem of elems) {
                    args.push(`${elem[key]}`, `${elem.id}`);
                }
                await this.redis.zadd(this.getIndexKey(key), ...args);
            } else if (this.schema[key].type === 'Date') {
                const args: string[] = [];
                for (let elem of elems) {
                    args.push(`${elem[key].getTime()}`, `${elem.id}`);
                }
                await this.redis.zadd(this.getIndexKey(key), ...args);
            }
        }
    }

    private async removeIndex(_elems: T[]) {
        for (let key of Object.keys(this.schema)) {
            if (!this.schema[key].index) continue;
            const elems = _elems.filter(elem => elem[key] !== undefined);
            if (!elems.length) continue;
            if (this.schema[key].type === 'string') {
                for (let elem of elems) {
                    const value: string = elem[key];
                    await this.redis.srem(
                        this.getStringIndexKey(key, value),
                        elem.id
                    );
                }
            } else if (this.schema[key].type === 'number') {
                const args: string[] = [];
                for (let elem of elems) {
                    args.push(`${elem.id}`);
                }
                await this.redis.zrem(this.getIndexKey(key), ...args);
            } else if (this.schema[key].type === 'Date') {
                const args: string[] = [];
                for (let elem of elems) {
                    args.push(`${elem.id}`);
                }
                await this.redis.zrem(this.getIndexKey(key), ...args);
            }
        }
    }

    private async insertFilter(_elems: T[]) {
        for (let filterName of Object.keys(this.filters)) {
            const { condition, orderKey } = this.filters[filterName];
            const elems = _elems.filter(condition);
            if (!elems.length) continue;
            if (!orderKey) {
                await this.redis.sadd(
                    this.getFilterKey(filterName),
                    ...elems.map(e => e.id)
                );
                continue;
            }
            if (this.schema[orderKey].type === 'string') {
                throw Error('string orderKey of filter is not supported');
            }

            let args: string[] = [];
            for (let elem of elems) {
                const score =
                    typeof elem[orderKey] === 'number'
                        ? elem[orderKey]
                        : elem[orderKey].getTime();
                args.push(score);
                args.push(`${elem.id}`);
            }
            await this.redis.zadd(this.getFilterKey(filterName), ...args);
        }
    }

    private async removeFilter(ids: (number | string)[]) {
        if (!ids.length) return;
        for (let filterName of Object.keys(this.filters)) {
            const { orderKey } = this.filters[filterName];

            if (!orderKey) {
                await this.redis.srem(this.getFilterKey(filterName), ...ids);
                continue;
            }

            await this.redis.zrem(this.getFilterKey(filterName), ...ids);
        }
    }

    private parseJSON(json: string) {
        const elem = JSON.parse(json);
        for (let key of this.dateKeys) {
            if (elem[key]) elem[key] = new Date(elem[key]);
        }
        return elem;
    }

    // MARK: public

    async insert(elem: T): Promise<T> {
        return (await this.insertMany([elem]))[0];
    }

    async remove(id: number | string) {
        await this.removeMany([id]);
    }

    async removeMany(ids: (number | string)[]) {
        const elems = await this.getMany(ids);
        await this.redis.del(...ids.map(id => this.getKey(id)));
        await this.removeIndex(elems);
        await this.removeFilter(ids);
    }

    async insertMany(_elems: T[]): Promise<T[]> {
        const elems = [..._elems];
        let lastId = this.autoId
            ? await this.redis.incrby(this.getCntKey(), elems.length)
            : 0;
        const args = [];
        let curId = lastId - elems.length;
        for (let elem of elems) {
            elem.id = ++curId;
            args.push(this.getKey(elem.id), JSON.stringify(elem));
        }

        await this.redis.mset(args);
        await this.insertIndex(elems);
        await this.insertFilter(elems);
        return elems;
    }

    async get(id: number | string): Promise<T> {
        return (await this.getMany([id]))[0];
    }

    async getMany(ids: (number | string)[]): Promise<T[]> {
        if (!ids.length) return [];
        const jsonList: string[] = await this.redis.mget(
            ...ids.map(id => this.getKey(id))
        );
        return jsonList.filter(json => json).map(json => this.parseJSON(json));
    }

    async find(key: string, value: any): Promise<T[]> {
        let ids;
        if (this.schema[key].type === 'string') {
            ids = await this.redis.smembers(this.getStringIndexKey(key, value));
            return await this.getMany(ids);
        }
        return await this.findRange(key, value, value);
    }

    async findRange(
        key: string,
        min: number | Date,
        max: number | Date
    ): Promise<T[]> {
        if (this.schema[key].type === 'string') {
            throw new Error('string findRange is not supported');
        }
        if (typeof min !== 'number') min = min.getTime();
        if (typeof max !== 'number') max = max.getTime();
        const ids = await this.redis.zrangebyscore(
            this.getIndexKey(key),
            min,
            max
        );
        return await this.getMany(ids);
    }

    async getFilteredList(
        filterName: string,
        min?: number | Date,
        max?: number | Date
    ) {
        let ids;
        const orderKey = this.filters[filterName].orderKey;
        if (this.schema[orderKey].type === 'string') {
            ids = await this.redis.smembers(this.getFilterKey(filterName));
        } else if (min === undefined || max === undefined) {
            ids = await this.redis.zrange(this.getFilterKey(filterName), 0, -1);
        } else {
            if (typeof min !== 'number') min = min.getTime();
            if (typeof max !== 'number') max = max.getTime();
            ids = await this.redis.zrangebyscore(
                this.getFilterKey(filterName),
                min,
                max
            );
        }

        return await this.getMany(ids);
    }
}
