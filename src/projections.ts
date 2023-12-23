import _ from 'lodash';

function getSqlOperator(o) {
    switch (o) {
        case '$eq':
            return '=';
        case '$lt':
            return '<';
        case '$lte':
            return '<=';
        case '$gt':
            return '>';
        case '$gte':
            return '>=';
        case '$in':
            return 'in';
        case '$and':
            return 'and';
        case '$or':
            return 'or';
        default:
            throw new Error(`Unrecognized operator '${o}'.`);
    }
}

function getOperand(o) {
    if (_.isString(o)) {
        return `'${o}'`;
    } else if (_.isNumber(o) || _.isBoolean(o)) {
        return `${o}`;
    } else if (_.isArray(o)) {
        if (o.length === 0) {
            return '[]';
        } else {
            return o.map((u) => getOperand(u)).join(', ');
        }
    }
}

/**
 * Converts the parsed Mongo-like projection to a Cypher constraint.
 * @param q {*} The result of a parse operation.
 * @param variable {string} The variable name to use.
 * @param level? {number} The iteration level.
 * @returns {string} A Cypher constraint (i.e. the Where part).
 */
export default function toSQL(q, variable = 'n', level = 0) {
    let s = '';
    if (q.operator) {
        if (q.field) {
            if (_.isArray(q.operand)) {
                // https://www.mongodb.com/docs/manual/reference/operator/query/all/
                if (q.operator === '$all') {
                    s += `(${getOperand(q.operand)} in ${variable}.${q.field || ''})`;
                } else {
                    s += `(${variable}.${q.field || ''} ${getSqlOperator(q.operator)} (${getOperand(q.operand)}))`;
                }
            } else {
                // https://www.mongodb.com/docs/manual/reference/operator/query/size/
                if (q.operator === '$size') {
                    s += `(length(${variable}.${q.field}) = ${getOperand(q.operand)})`;
                }
                // note: this deviates from the Mongo book and is an addition to the syntax
                else if (q.operator === '$startsWith') {
                    s += `(${variable}.${q.field} like '${q.operand}%')`;
                }
                // note: this deviates from the Mongo book and is an addition to the syntax
                else if (q.operator === '$contains') {
                    s += `(${variable}.${q.field} like '%${q.operand}%')`;
                } else if (q.operator === '$regex') {
                    s += `(${variable}.${q.field} regexp '${q.operand}')`;
                } else {
                    s += `(${variable}.${q.field || ''} ${getSqlOperator(q.operator)} ${getOperand(q.operand)})`;
                }
            }
        } else {
            const conjunctor = getSqlOperator(q.operator);
            const coll:string[] = [];
            for (const part of q.parts) {
                coll.push(toSQL(part, variable));
            }
            s += coll.join(` ${conjunctor} `);
            return s;
        }
    }

    if (q.parts.length > 0) {
        for (const u of q.parts) {
            s += toSQL(u, variable, level + 1);
        }
    }
    return s;
}
