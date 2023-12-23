import { DataTypes, Model, Sequelize } from "sequelize";
import { Utils } from "@orbifold/utils";
import _ from "lodash";

function parseSchemaOptions(options) {
	const defaultSequelizeOptions = "sqlite:memory";
	const defaultOtherOptions = { recreateTables: false };
	let sequelizeOptions;
	let otherOptions;
	if (Utils.isEmpty(options)) {
		sequelizeOptions = _.clone(defaultSequelizeOptions);
		otherOptions = _.clone(defaultOtherOptions);
	} else {
		if (_.isString(options)) {
			sequelizeOptions = options;
			otherOptions = _.clone(defaultOtherOptions);
		} else if (_.isPlainObject(options)) {
			otherOptions = {};
			_.forEach(defaultOtherOptions, (v, k) => {
				otherOptions[k] = options[k] || defaultOtherOptions[k];
				delete options[k];
			});
			// whatever remains are supposedly for sequelize
			sequelizeOptions = options;
			if (Utils.isEmpty(sequelizeOptions)) {
				sequelizeOptions = _.clone(defaultSequelizeOptions);
			}
		}
	}
	return [sequelizeOptions, otherOptions];
}

/**
 * This sets up the relational schema holding the graph.
 * The returned Node and Edge objects can be used to create things.
 * @see https://sequelize.org/docs/v6/core-concepts/model-instances/#a-very-useful-shortcut-the-create-method
 * @param options {*} The Sequelize connection definition.
 * @returns {Promise<{Node: void, Edge: void, db}>}
 */
export default async function setupSqliteSchema(options) {
	const [sequelizeOptions, otherOptions] = parseSchemaOptions(options);
	const db = new Sequelize(sequelizeOptions);
	const Node = await db.define("Node", {
		id: {
			type: DataTypes.STRING,
			allowNull: false,
			primaryKey: true,
		},
		labels: DataTypes.STRING,
		data: DataTypes.JSON,
	});
	const Edge = await db.define("Edge", {
		id: {
			type: DataTypes.STRING,
			allowNull: false,
			primaryKey: true,
		},
		sourceId: {
			type: DataTypes.STRING,
			allowNull: false,
		},
		targetId: {
			type: DataTypes.STRING,
			allowNull: false,
		},
		labels: DataTypes.STRING,
		data: DataTypes.JSON,
	});

	Edge.belongsTo(Node, {
		as: "Source",
		foreignKey: "sourceId",
		constraints: true,
	});
	Edge.belongsTo(Node, {
		as: "Target",
		foreignKey: "targetId",
		constraints: true,
	});

	// using 'force: true' will recreate the tables each time
	await db.sync({ force: otherOptions.recreateTables });
	return { db, Node, Edge };
}
