import { assert } from 'chai';
import * as Redis from 'ioredis';
import { RedisRichStructure } from '../';
import * as _ from 'lodash';
import * as mysql from 'promise-mysql';

interface Car {
    id?: number;
    type?: string;
    weight?: number;
    createdAt?: Date;
}

const redis = new Redis();
describe('RedisRichStructure', () => {
    const redisCars = new RedisRichStructure<Car>(
        redis,
        'cars',
        {
            id: 0,
            type: '',
            weight: 0,
            createdAt: new Date(),
        },
        ['id', 'type', 'weight', 'createdAt'],
        {
            filter1: {
                orderKey: 'id',
                condition: (elem: Car) => elem.type === 'hoge1',
            },
            filter2: {
                orderKey: 'weight',
                condition: (elem: Car) =>
                    elem.type === 'hoge1' && !!elem.weight,
            },
            filter3: {
                orderKey: 'createdAt',
                condition: (elem: Car) =>
                    elem.type === 'hoge1' && !!elem.createdAt,
            },
            filter4: {
                orderKey: 'createdAt',
                condition: (elem: Car) =>
                    elem.type === 'hoge2' && !!elem.createdAt,
            },
        }
    );

    const now1 = new Date();
    const now3 = new Date(new Date().getTime() + 10000);
    const originalCars = [
        {
            id: 100,
            type: 'hoge1',
            weight: 300,
            createdAt: now1,
        },
        {
            id: 2,
            type: 'hoge2',
            weight: 200,
        },
        {
            id: 3,
            type: 'hoge1',
            createdAt: now3,
        },
    ];

    beforeEach(async () => {
        await redis.flushdb();
    });

    it('should insert/get value', async () => {
        const { id } = await redisCars.insert(originalCars[0]);
        const car = await redisCars.findById(id!);
        assert.deepStrictEqual(car, { ...originalCars[0], id });
    });

    it('should insert/delete/get value', async () => {
        const { id } = await redisCars.insert(originalCars[0]);
        assert.deepStrictEqual(await redisCars.findById(id!), {
            ...originalCars[0],
            id,
        });
        await redisCars.remove(id!);
        assert.deepStrictEqual(await redisCars.findById(id!), undefined);
        await redisCars.insert(
            {
                ...originalCars[0],
                id,
            },
            false
        );
        assert.deepStrictEqual(await redisCars.findById(id!), {
            ...originalCars[0],
            id,
        });
    });

    it('should upsert value', async () => {
        const { id } = await redisCars.insert(originalCars[0]);
        assert.deepStrictEqual(await redisCars.findById(id!), {
            ...originalCars[0],
            id,
        });
        await redisCars.upsert({
            ...originalCars[0],
            id,
            weight: 222,
        });
        await redisCars.upsert(originalCars[1]);
        assert.deepStrictEqual(await redisCars.findById(id!), {
            ...originalCars[0],
            id,
            weight: 222,
        });
        assert.deepStrictEqual(
            await redisCars.findById(originalCars[1].id),
            originalCars[1]
        );
    });

    it('should insert/get optional value', async () => {
        const { id } = await redisCars.insert({});
        const car = await redisCars.findById(id!);
        assert.deepStrictEqual(car, { id });
    });

    it('should insert/get multiple values', async () => {
        const insertedCars = await redisCars.insertMany(originalCars);
        const ids: number[] = insertedCars.map(c => c.id!);
        assert.deepStrictEqual(ids, [1, 2, 3]);
        const cars = await redisCars.findByIds(ids);
        assert.deepStrictEqual(cars, originalCars);
    });

    it('should find by key', async () => {
        await redisCars.insertMany(originalCars);

        assert.deepStrictEqual(await redisCars.findBy('type', 'dummy'), []);

        const hoge1Cars = await redisCars.findBy('type', 'hoge1');
        hoge1Cars.sort((a: Car, b: Car) => a.id! - b.id!);
        assert.deepStrictEqual(hoge1Cars, [originalCars[0], originalCars[2]]);

        const weight100Cars = await redisCars.findBy('weight', 300);
        assert.deepStrictEqual(weight100Cars, [originalCars[0]]);

        const createdAtNow3Cars = await redisCars.findBy('createdAt', now3);
        assert.deepStrictEqual(createdAtNow3Cars, [originalCars[2]]);
    });

    it('should find by range key', async () => {
        await redisCars.insertMany(originalCars);
        assert.deepStrictEqual(
            await redisCars.findRangeBy('weight', 50, 150),
            []
        );
        assert.deepStrictEqual(
            await redisCars.findRangeBy('weight', 299, 301),
            [originalCars[0]]
        );
        assert.deepStrictEqual(
            await redisCars.findRangeBy('weight', -50, 450),
            [originalCars[1], originalCars[0]]
        );
    });

    it('should filter', async () => {
        await redisCars.insertMany(originalCars);
        assert.deepStrictEqual(await redisCars.findByFilter('filter1'), [
            originalCars[0],
            originalCars[2],
        ]);
        assert.deepStrictEqual(await redisCars.findByFilter('filter2'), [
            originalCars[0],
        ]);
        assert.deepStrictEqual(await redisCars.findByFilter('filter3'), [
            originalCars[0],
            originalCars[2],
        ]);
        assert.deepStrictEqual(await redisCars.findByFilter('filter4'), []);
    });

    it('should delete element', async () => {
        await redisCars.insertMany(originalCars);
        assert.deepStrictEqual(
            await redisCars.findRangeBy('id', -1, 100),
            originalCars
        );
        await redisCars.remove(2);
        await redisCars.remove(2);
        assert.deepStrictEqual(await redisCars.findRangeBy('id', -1, 100), [
            originalCars[0],
            originalCars[2],
        ]);
        assert.deepStrictEqual(await redisCars.findByIds([1, 2, 3]), [
            originalCars[0],
            originalCars[2],
        ]);
        await redisCars.removeMany([1, 2, 3]);
        assert.deepStrictEqual(await redisCars.findBy('type', 'hoge1'), []);
        assert.deepStrictEqual(
            await redisCars.findRangeBy('weight', -1, 100),
            []
        );
        assert.deepStrictEqual(
            await redisCars.findRangeBy('createdAt', now1, now3),
            []
        );
    });

    it('should insert many, delete many, get many, filter correctly', async () => {
        const carsWithoutId: Car[] = [];
        for (let i = 0; i < 100; i++) {
            carsWithoutId.push({
                type: `hoge${rand(0, 10)}`,
                weight: rand(100, 200),
                createdAt: new Date(new Date().getTime() + rand(0, 100)),
            });
        }
        let cars = await redisCars.insertMany(carsWithoutId);
        assert.sameDeepMembers(
            await redisCars.findBy('type', 'hoge4'),
            cars.filter(car => car.type === 'hoge4')
        );
        const removedIds: number[] = [];
        for (let i = 0; i < 20; i++) removedIds.push(rand(0, 100));
        await redisCars.removeMany(removedIds);
        cars = cars.filter(car => !removedIds.includes(car.id!));
        assert.sameDeepMembers(
            await redisCars.findBy('type', 'hoge4'),
            cars.filter(car => car.type === 'hoge4')
        );

        assert.deepEqual(
            await redisCars.findByFilter('filter1'),
            cars.filter((elem: Car) => elem.type === 'hoge1')
        );
        assert.deepEqual(
            await redisCars.findRangeByFilter('filter1', 10, 20),
            cars.filter(
                (elem: Car) =>
                    elem.type === 'hoge1' && elem.id! >= 10 && elem.id! <= 20
            )
        );
        assert.deepEqual(
            await redisCars.findByFilter('filter2'),
            _.sortBy(
                cars.filter(
                    (elem: Car) => elem.type === 'hoge1' && elem.weight
                ),
                'weight'
            )
        );
        assert.deepEqual(
            await redisCars.findRangeByFilter('filter2', 150, 180),
            _.sortBy(
                cars.filter(
                    (elem: Car) =>
                        elem.type === 'hoge1' &&
                        elem.weight &&
                        elem.weight >= 150 &&
                        elem.weight <= 180
                ),
                'weight'
            )
        );
        assert.deepEqual(
            await redisCars.findByFilter('filter3'),
            _.sortBy(
                cars.filter((elem: Car) => elem.type === 'hoge1'),
                'createdAt'
            )
        );
    });
});

const BULK_NUM = 500;
describe('performance', function() {
    this.timeout(1000000);
    const redisCars = new RedisRichStructure<Car>(
        redis,
        'cars',
        {
            id: 0,
            type: '',
            weight: 0,
            createdAt: new Date(),
        },
        ['id', 'type', 'weight', 'createdAt'],
        {
            filter2: {
                orderKey: 'weight',
                condition: (elem: Car) =>
                    elem.type === 'hoge1' && !!elem.weight,
            },
        }
    );

    before(async () => {
        await redis.flushdb();
    });

    it('should insert many', async () => {
        const cars: Car[] = [];
        for (let i = 0; i < BULK_NUM; i++) {
            cars.push({
                type: `hoge-${rand(0, 10)}`,
                weight: rand(100, 200),
                createdAt: new Date(new Date().getTime() + rand(0, 100)),
            });
        }
        await redisCars.insertMany(cars);
    });

    it('should insert one by one', async () => {
        for (let i = 0; i < BULK_NUM; i++) {
            await redisCars.insert({
                type: `hoge-${rand(0, 10)}`,
                weight: rand(100, 200),
                createdAt: new Date(new Date().getTime() + rand(0, 100)),
            });
        }
    });

    it('find many', async () => {
        for (let i = 0; i < BULK_NUM; i++) {
            await redisCars.findBy('type', 'hoge-1');
        }
        for (let i = 0; i < BULK_NUM; i++) {
            await redisCars.findBy('weight', 200);
        }
    });

    it('find ids many', async () => {
        for (let i = 0; i < BULK_NUM; i++) {
            await redisCars.findIdsBy('type', 'hoge1');
        }
    });

    it('should remove many', async () => {
        const ids: number[] = [];
        for (let i = 0; i < BULK_NUM; i += 2) {
            ids.push(i);
        }
        await redisCars.removeMany(ids);
    });
});

describe('mysql', () => {
    let connection: mysql.Connection;
    before(async () => {
        connection = await mysql.createConnection({
            host: 'localhost',
            user: 'root',
            password: '',
            database: 'redis_bench_db',
        });
    });
    it('should insert many', async () => {
        for (let i = 0; i < BULK_NUM; i++) {
            const type = `hoge-${rand(0, 10)}`;
            const weight = rand(100, 200);
            const createdAt = new Date(new Date().getTime() + rand(0, 100));
            await connection.query(
                'INSERT INTO car (id, type, weight, createdAt) VALUES (?, ?, ?, ?)',
                [i, type, weight, createdAt]
            );
            // console.log({ ret });
        }
    }).timeout(10000);

    it('find many', async () => {
        for (let i = 0; i < BULK_NUM; i++) {
            await connection.query('SELECT * FROM car WHERE type = ?', [
                'hoge-1',
            ]);
        }
        for (let i = 0; i < BULK_NUM; i++) {
            await connection.query('SELECT * FROM car WHERE weight = ?', [200]);
        }
    }).timeout(10000);

    it('find ids many', async () => {
        for (let i = 0; i < BULK_NUM; i++) {
            await connection.query('SELECT id FROM car WHERE type = ?', [
                'hoge-1',
            ]);
        }
    }).timeout(10000);
});

function rand(min: number, max: number) {
    return Math.floor(Math.random() * (max - min)) + min;
}
