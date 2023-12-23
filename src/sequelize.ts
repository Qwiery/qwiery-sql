import _ from "lodash";
import { DataTypes, Model, Op, Sequelize } from "sequelize";

import { Utils } from "@orbifold/utils";

function getSequelizeOperator(operator, parentKey = null) {
	switch (operator) {
		case "$eq":
			return Op.eq;
		case "$lt":
			return Op.lt;
		case "$lte":
			return Op.lte;
		case "$gt":
			return Op.gt;
		case "$gte":
			return Op.gte;
		case "$in":
			return Op.in;
		case "$and":
			return Op.and;
		case "$or":
			return Op.or;
		case "$startsWith":
			return Op.startsWith;
		case "$contains":
			return Op.substring;
		case "$size":
			throw new Error("Should have been handled separately already.");
		default:
			if (_.includes(["id", "labels"], operator)) {
				return operator;
			} else {
				return `data.${operator}`;
			}
	}
}

/**
 * Converts the Mongo-like projection to a Sequelize projection.
 * The two dialects are very similar but the operator keywords in Sequelize are Symbols rather than strings.
 * @param q {*} The result of a parse operation.
 * @param level? {number} The iteration level.
 * @returns {*|*[]} One or more Sequelize constraints (i.e. the Where part).
 */
export default function toSequelize(projector) {
	let specialCases = []; // collection of 'Op.and' constraints
	const p = rewrite(projector, specialCases);
	if (specialCases.length === 0) {
		// strangely enough, something like Object.keys({[Op.gte]: 3}) is []
		if (_.isNil(p) || (_.keys(p).length === 0 && Object.getOwnPropertySymbols(p).length === 0)) {
			return {};
		} else {
			return { where: p };
		}
	} else {
		if (_.isNil(p) || _.keys(p).length === 0) {
			return { where: specialCases };
		} else {
			specialCases.push(p);
			return { where: specialCases };
		}
	}
}

function rewrite(projector, specialCases = []) {
	const r = {};
	for (const key of Object.keys(projector)) {
		const newKey = getSequelizeOperator(key);
		const value = projector[key];

		if (_.isPlainObject(value)) {
			// look ahead to see if there is a $size which requires special handling
			let addKey = true;
			for (const k of _.keys(value)) {
				if (_.includes(["$size"], k)) {
					specialCases.push(Sequelize.where(Sequelize.fn("json_array_length", Sequelize.col("data"), Sequelize.literal(`"\$.${key}"`)), value[k]));
					addKey = false;
					break;
				}
			}
			if (addKey) {
				r[newKey] = rewrite(value, specialCases);
			}
		} else if (_.isArray(value)) {
			r[newKey] = value.map((u) => rewrite(u, specialCases));
		} else {
			r[newKey] = value;
		}
	}
	return r;
}
