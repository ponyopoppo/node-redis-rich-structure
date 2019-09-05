import * as Redis from 'ioredis';

type SchemaType = 'string' | 'number' | 'Date';
interface SchemaEntry {
    type: SchemaType;
    index?: boolean;
}

type Schema<T> = { [Key in keyof T]: SchemaEntry };

type Filter<T> = {
    [key: string]: {
        orderKey: keyof T;
        condition: (elem: T) => boolean;
    };
};

type IdType = string | number;

export class RedisRichStructure<
    T extends { id?: IdType },
    NewT = Omit<T, 'id'> & { id?: IdType }
> {
    private dateKeys: string[];
    private schema: Schema<T> = {} as any;
    constructor(
        private redis: Redis.Redis,
        private collectionName: string,
        defaultValue: T,
        indexes: (keyof T)[],
        private filters: Filter<T>
    ) {
        for (let key of Object.keys(defaultValue)) {
            const value = defaultValue[key];
            const type =
                typeof value === 'string'
                    ? 'string'
                    : typeof value === 'number'
                    ? 'number'
                    : 'Date';
            this.schema[key] = { type };
        }
        for (let key of indexes) this.schema[key].index = true;

        this.dateKeys = Object.keys(this.schema).filter(
            key => this.schema[key].type === 'Date'
        );
    }

    private clone(array: any[]) {
        for (let i = 0; i < array.length; i++) {
            array[i] = { ...array[i] };
        }
        return [...array];
    }

    private getCntKey() {
        return `idcnt::${this.collectionName}`;
    }

    private getKey(id: IdType) {
        return `${this.collectionName}:${id}`;
    }

    private getIndexKey(key: keyof T) {
        return `index::${key}`;
    }

    private getStringIndexKey(key: keyof T, value: string) {
        return `index::${key}:${value}`;
    }

    private getFilterKey(filterName: string) {
        return `filter::${filterName}`;
    }

    private async insertIndex(_elems: T[]) {
        for (let _key of Object.keys(this.schema)) {
            const key = _key as keyof T;
            if (!this.schema[key].index) continue;
            const elems = _elems.filter(elem => elem[key] !== undefined);
            if (!elems.length) continue;
            if (this.schema[key].type === 'string') {
                for (let elem of elems) {
                    if (elem[key] === undefined) continue;
                    const value: string = elem[key] as any;
                    await this.redis.sadd(
                        this.getStringIndexKey(key, value),
                        elem.id
                    );
                }
            } else if (this.schema[key].type === 'number') {
                const args: string[] = [];
                for (let elem of elems) {
                    if (elem[key] === undefined) continue;
                    args.push(`${elem[key]}`, `${elem.id}`);
                }
                if (!args.length) continue;
                await this.redis.zadd(this.getIndexKey(key), ...args);
            } else if (this.schema[key].type === 'Date') {
                const args: string[] = [];
                for (let elem of elems) {
                    if (elem[key] === undefined) continue;
                    args.push(`${(elem[key] as any).getTime()}`, `${elem.id}`);
                }
                if (!args.length) continue;
                await this.redis.zadd(this.getIndexKey(key), ...args);
            }
        }
    }

    private async removeIndex(_elems: T[]) {
        for (let _key of Object.keys(this.schema)) {
            const key = _key as keyof T;
            if (!this.schema[key].index) continue;
            const elems = _elems.filter(elem => elem[key] !== undefined);
            if (!elems.length) continue;
            if (this.schema[key].type === 'string') {
                for (let elem of elems) {
                    const value: string = elem[key] as any;
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
                        : (elem[orderKey] as any).getTime();
                args.push(score);
                args.push(`${elem.id}`);
            }
            await this.redis.zadd(this.getFilterKey(filterName), ...args);
        }
    }

    private async removeFilter(ids: IdType[]) {
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

    async insert(elem: NewT, autoIncId: boolean = true): Promise<T> {
        return (await this.insertMany([elem], autoIncId))[0];
    }

    async insertMany(_elems: NewT[], autoIncId: boolean = true): Promise<T[]> {
        const elems: T[] = this.clone(_elems) as any;
        let lastId = autoIncId
            ? await this.redis.incrby(this.getCntKey(), elems.length)
            : 0;
        const args = [];
        let curId = lastId - elems.length;
        for (let elem of elems) {
            if (autoIncId) elem.id = ++curId;
            else if (elem.id === undefined)
                throw new Error('Element id is necessary');
            args.push(this.getKey(elem.id), JSON.stringify(elem));
        }

        await this.redis.mset(args);
        await this.insertIndex(elems);
        await this.insertFilter(elems);
        return elems;
    }

    async remove(id: IdType) {
        await this.removeMany([id]);
    }

    async removeMany(ids: IdType[]) {
        const elems = await this.findByIds(ids);
        await this.redis.del(...ids.map(id => this.getKey(id)));
        await this.removeIndex(elems);
        await this.removeFilter(ids);
    }

    async upsert(elem: T) {
        return (await this.upsertMany([elem]))[0];
    }

    async upsertMany(elems: T[]) {
        await this.removeMany(elems.map(elem => elem.id!));
        return this.insertMany(elems as any[], false);
    }

    async findById(id: IdType): Promise<T> {
        return (await this.findByIds([id]))[0];
    }

    async findByIds(ids: IdType[]): Promise<T[]> {
        if (!ids.length) return [];
        const jsonList: string[] = await this.redis.mget(
            ...ids.map(id => this.getKey(id))
        );
        return jsonList.filter(json => json).map(json => this.parseJSON(json));
    }

    async findBy(key: keyof T, value: any): Promise<T[]> {
        return this.findByIds(await this.findIdsBy(key, value));
    }

    async findIdsBy(key: keyof T, value: any): Promise<IdType[]> {
        if (!this.schema[key].index) throw new Error(`${key} is not indexed`);
        if (this.schema[key].type === 'string') {
            return await this.redis.smembers(
                this.getStringIndexKey(key, value)
            );
        }
        return this.findIdsRangeBy(key, value, value);
    }

    async findRangeBy(
        key: keyof T,
        min: number | Date,
        max: number | Date
    ): Promise<T[]> {
        return this.findByIds(await this.findIdsRangeBy(key, min, max));
    }

    async findIdsRangeBy(
        key: keyof T,
        min: number | Date,
        max: number | Date
    ): Promise<(number | string)[]> {
        if (!this.schema[key].index) throw new Error(`${key} is not indexed`);
        if (this.schema[key].type === 'string') {
            throw new Error('string findRange is not supported');
        }
        if (typeof min !== 'number') min = min.getTime();
        if (typeof max !== 'number') max = max.getTime();
        return this.redis.zrangebyscore(this.getIndexKey(key), min, max);
    }

    async findByFilter(filterName: string) {
        return this.findByIds(
            await this.redis.zrange(this.getFilterKey(filterName), 0, -1)
        );
    }

    async findRangeByFilter(
        filterName: string,
        min: number | Date,
        max: number | Date
    ) {
        let ids;
        const orderKey = this.filters[filterName].orderKey;
        if (!this.schema[orderKey].index)
            throw new Error(`${orderKey} is not indexed`);
        if (this.schema[orderKey].type === 'string') {
            ids = await this.redis.smembers(this.getFilterKey(filterName));
        } else {
            if (typeof min !== 'number') min = min.getTime();
            if (typeof max !== 'number') max = max.getTime();
            ids = await this.redis.zrangebyscore(
                this.getFilterKey(filterName),
                min,
                max
            );
        }

        return await this.findByIds(ids);
    }
}
