import {describe, test, it, expect} from 'vitest';
import {parseProjection} from "@orbifold/projections";
import toSQL from '../src/projections';
import toSequelize from '../src/sequelize';
import _ from 'lodash';
import {DataTypes, Model, Op, Sequelize} from 'sequelize';

describe('MongoParser', function () {
    it('should convert basic constructs', function () {
        let tree = parseProjection({x: 'R'});
        expect(toSQL(tree, 'u')).toEqual("(u.x = 'R')");

        tree = parseProjection({x: {$eq: 12}});
        expect(toSQL(tree)).toEqual('(n.x = 12)');

        tree = parseProjection({x: {$lt: 12}});
        expect(toSQL(tree)).toEqual('(n.x < 12)');

        tree = parseProjection({x: {$gte: 12}});
        expect(toSQL(tree)).toEqual('(n.x >= 12)');

        tree = parseProjection({x: {$in: [1, 2]}});
        expect(toSQL(tree)).toEqual('(n.x in (1, 2))');

        tree = parseProjection({x: {$in: ['a', 'b']}});
        expect(toSQL(tree)).toEqual("(n.x in ('a', 'b'))");

        // the labels containing the label A
        tree = parseProjection({labels: {$all: ['A']}});
        expect(toSQL(tree)).toEqual("('A' in n.labels)");
    });

    it('should do more complex things too', function () {
        let tree = parseProjection({$or: [{x: 'R'}, {x: 'S'}]});
        expect(toSQL(tree, 'u')).toEqual("(u.x = 'R') or (u.x = 'S')");

        tree = parseProjection({$and: [{x: {$lt: 45}}, {x: 'S'}]});
        expect(toSQL(tree, 'u')).toEqual("(u.x < 45) and (u.x = 'S')");

        tree = parseProjection({v: {$size: 13}});
        expect(toSQL(tree, 'u')).toEqual('(length(u.v) = 13)');

        tree = parseProjection({s: {$contains: 'aa'}});
        expect(toSQL(tree, 'w')).toEqual("(w.s like '%aa%')");

        tree = parseProjection({s: {$startsWith: 'f'}});
        expect(toSQL(tree, 'w')).toEqual("(w.s like 'f%')");
    });

    it('should sequelize Mongo projections', function () {
        let p = toSequelize({a: 3});
        expect(p).toEqual({where: {'data.a': 3}});

        p = toSequelize({});
        expect(p).toEqual({});

        p = toSequelize({id: 3});
        expect(p).toEqual({where: {id: 3}});

        p = toSequelize({labels: 'c,v'});
        expect(p).toEqual({where: {labels: 'c,v'}});

        p = toSequelize({a: {$eq: 4}});
        expect(p).toEqual({where: {'data.a': {[Op.eq]: 4}}});

        p = toSequelize({a: {$contains: 4}});
        expect(p).toEqual({where: {'data.a': {[Op.substring]: 4}}});

        p = toSequelize({a: {$size: 4}, b: 3});
        expect(p.where).toBeInstanceOf(Array);
        expect(p.where).toHaveLength(2);
        expect(p.where[1]).toEqual({'data.b': 3});
        expect(_.isObject(p.where[0])).toBeTruthy();
        expect(p.where[0].constructor.name).toEqual('Where');
    });
});
