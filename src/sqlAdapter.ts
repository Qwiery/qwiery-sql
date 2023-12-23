import { Utils } from "@orbifold/utils";
import * as process from "process";
import _ from "lodash";
import AdapterUtils from "./utils.js";
import setupSqliteSchema from "./sqlSchema.js";
import toSequelize from "./sequelize.js";
import { Error, Op } from "sequelize";
import { Graph } from "@orbifold/graphs";

const DefaultOptions = {
	defaultNodeLabel: "Thing",
	defaultEdgeLabel: "RelatedTo",
};
const AdapterId = "sql";
export   async function SqlAdapter(options, done) {
	const qwiery = this;
	let driver = null;
	let error = null;
	let isInitialized = false;
	let db, N, E;

	//region Local wrappers
	async function getNodeById(id) {
		const r = await N.findOne({ where: { id } });
		if (_.isNil(r)) {
			return null;
		} else {
			return AdapterUtils.dbRecordToNode(r);
		}
	}

	/**
	 * The method exists below as an adapter override but this one is somewhat easier to use and appears
	 * in pretty much all methods.
	 * @param id
	 * @returns {Promise<boolean>}
	 */
	async function nodeExists(id) {
		try {
			return (await N.findOne({ where: { id } })) != null;
		} catch (e:any) {
			return false;
		}
	}

	async function edgeExists(id) {
		try {
			return (await E.findOne({ where: { id } })) != null;
		} catch (e:any) {
			return false;
		}
	}

	async function getNodeLabels() {
		const found = await N.findAll({
			attributes: ["labels"],
			group: ["labels"],
		});
		const labels = found.map((project) => project.labels);
		const all:string[]  = [];
		for (const label of labels) {
			if (label.indexOf(",") > -1) {
				const parts = label
					.split(",")
					.map((s) => s.trim())
					.filter((s) => s.length > 0);
				all.push(...parts);
			} else {
				all.push(label);
			}
		}
		return _.uniq(all);
	}

	async function getNodeLabelProperties(labelName) {
		const nodes = await getNodesWithLabel(labelName);
		const propNames:string[]  = [];
		for (const node of nodes) {
			const names = Object.keys(node);
			names.forEach((name) => {
				if (name !== "labels" && !_.includes(propNames, name)) {
					propNames.push(name);
				}
			});
		}

		return propNames;
	}

	async function getEdgesWithLabels(sourceLabel, edgeLabel, targetLabel, amount = 1000) {
		if (Utils.isEmpty(sourceLabel)) {
			throw new Error("No source label given.");
		}
		if (Utils.isEmpty(edgeLabel)) {
			throw new Error("No edge label given.");
		}
		if (Utils.isEmpty(targetLabel)) {
			throw new Error("No target label given.");
		}
		if (edgeLabel === "*") {
			return await getEdgesBetweenLabels(sourceLabel, targetLabel, amount);
		}

		const coll:any[] = [];
		const edges = await getEdgesWithLabel(edgeLabel, amount);
		if (sourceLabel === "*" && targetLabel === "*") {
			return edges;
		}

		for (const edge of edges) {
			if (sourceLabel !== "*") {
				const sourceNode = await getNodeById(edge.sourceId);
				if (!_.includes(sourceNode!.labels, sourceLabel)) {
				} else {
					if (targetLabel !== "*") {
						const targetNode = await getNodeById(edge.targetId);
						if (!_.includes(targetNode!.labels, targetLabel)) {
						} else {
							coll.push(edge);
						}
					} else {
						coll.push(edge);
					}
				}
			} else {
				if (targetLabel !== "*") {
					const targetNode = await getNodeById(edge.targetId);
					if (!_.includes(targetNode!.labels, targetLabel)) {
					} else {
						coll.push(edge);
					}
				} else {
					coll.push(edge);
				}
			}
		}

		return coll;
	}

	async function getNodesWithLabel(label, amount = 1000) {
		const records = await N.findAll({ where: { labels: { [Op.substring]: label } }, limit: amount });
		if (records.length > 0) {
			const nodes = records.map((r) => AdapterUtils.dbRecordToNode(r));
			return nodes;
		} else {
			return [];
		}
	}

	async function getEdgesWithLabel(label, amount = 1000) {
		const records = await E.findAll({ where: { labels: { [Op.substring]: label } }, limit: amount });
		if (records.length > 0) {
			const edges = records.map((r) => AdapterUtils.dbRecordToEdge(r));
			return edges;
		} else {
			return [];
		}
	}

	async function getEdgesBetweenLabels(sourceLabel, targetLabel, amount = 1000) {
		const coll:any[] = [];
		const sourceNodes = await getNodesWithLabel(sourceLabel, amount);
		for (const sourceNode of sourceNodes) {
			const edges = await getDownstreamEdges(sourceNode.id);
			for (const edge of edges) {
				const targetNode = await getNodeById(edge.targetId);
				if (_.includes(targetNode!.labels, targetLabel)) {
					coll.push(edge);
				}
			}
		}
		return coll;
	}

	async function getDownstreamEdges(sourceId, amount = 1000) {
		const records = await E.findAll({ where: { sourceId }, limit: amount });
		if (records.length > 0) {
			const edges = records.map((r) => AdapterUtils.dbRecordToEdge(r));
			return edges;
		} else {
			return [];
		}
	}

	async function getUpstreamEdges(targetId, amount = 1000) {
		const records = await E.findAll({ where: { targetId }, limit: amount });
		if (records.length > 0) {
			const edges = records.map((r) => AdapterUtils.dbRecordToEdge(r));
			return edges;
		} else {
			return [];
		}
	}

	/**
	 * Things are alot easier if you have Cypher or similar, so this is the tedious path towards path querying.
	 *
	 * The approach is as follows:
	 * - an initial graph is assembled by looking at each segment of the path (a segment being a triple)
	 * - a depth-first traversal collects the nodes being part of the full path query
	 * - the nodes not part of the full path are removed.
	 *
	 * This is to make sure that partial paths are not part of the resulting graph.
	 * A path query means an "AND" constraint.
	 * @param path
	 * @param amount
	 * @returns {Promise<Graph>}
	 */
	async function pathQuery(path, amount = 1000) {
		if (Utils.isEmpty(path)) {
			return Graph.empty();
		}
		if (path.length % 2 === 0) {
			throw new Error("A path query should have odd length.");
		}
		// check for valid path elements
		for (const s of path) {
			if (!_.isString(s) || Utils.isEmpty(s)) {
				throw new Error("Invalid path query.");
			}
		}
		const g = new Graph();
		if (path.length === 1) {
			const nodes = await getNodesWithLabel(path[0], amount);
			g.addNodes(nodes);
			return g;
		}

		/**
		 * Adds the nodes and edges fitting the given triple.
		 */
		const mergeSegment = async (segment) => {
			let edges = await getEdgesWithLabels(...segment, amount);
			for (const edge of edges) {
				const targetNode = await getNodeById(edge.targetId);
				if (!g.nodeIdExists(edge.sourceId)) {
					const sourceNode = await getNodeById(edge.sourceId);
					g.addNode(sourceNode);
				}
				if (!g.nodeIdExists(edge.targetId)) {
					const targetNode = await getNodeById(edge.targetId);
					g.addNode(targetNode);
				}
				if (!g.edgeExists(edge.id)) {
					g.addEdge(edge);
				}
			}
		};

		/**
		 * Assembles the initial graph based on the path labels.
		 */
		const assembleRawGraph = async () => {
			const segments = _.range((path.length - 1) / 2).map((i) => [path[2 * i], path[2 * i + 1], path[2 * i + 2]]);
			for (const segment of segments) {
				await mergeSegment(segment);
			}
		};

		/**
		 * DFT and collects the valid node-paths.
		 * @param n Node
		 * @param edgeLabels The edge-labels which have to be traverse down.
		 * @param pathsFound The paths found so far.
		 * @param nodePath The current parents.
		 */
		const traverse = (n, edgeLabels, pathsFound, nodePath = []) => {
			nodePath.push(n);
			if (edgeLabels.length === 0) {
				pathsFound.push(nodePath);
				return;
			}
			const edgeLabel = edgeLabels.shift();
			let edges = g.getOutgoingEdges(n.id);
			if (edgeLabel !== "*") {
				edges = edges.filter((e:any) => _.includes(e.labels, edgeLabel));
			}
			const children = edges.map((e:any) => g.getNodeById(e.targetId));
			if (children.length > 0) {
				for (const child of children) {
					traverse(child, _.clone(edgeLabels), pathsFound, _.clone(nodePath));
				}
			} else {
				// unfinished path
			}
		};

		/**
		 * DFT according to the given edge-label path and tags the nodes which are part of a full path.
		 */
		const walkDownAdnTag = (startNode, edgeLabels) => {
			if (edgeLabels.length === 0) {
				return;
			}
			const pathsFound:any[]  = [];
			traverse(startNode, _.clone(edgeLabels), pathsFound);
			// also truncating to the nearest amount of nodes while preserving paths

			pathsFound.forEach((p) => {
				p.forEach((n) => (n._drop = false));
			});
		};

		/**
		 * Removes the nodes which are not in the given path.
		 */
		const pruneRawGraph = async () => {
			// start from the nodes and tag all that are part of the path downwards
			g.nodes.forEach((n) => (n._drop = true));

			const edgeLabels = _.range((path.length - 1) / 2).map((i) => path[2 * i + 1]);
			for (const n of g.nodes) {
				// walking down and tagging
				walkDownAdnTag(n, edgeLabels);
			}
			for (const n of g.nodes) {
				if (n._drop) {
					g.removeNode(n);
				} else {
					delete n._drop;
				}
			}
		};

		await assembleRawGraph();
		await pruneRawGraph();
		return g;
	}

	async function getSchemaRecords() {
		const found = await db.query("select distinct S.labels L1, E.labels L, T.labels L2 from Edges E inner join Nodes S on E.sourceId=S.id inner join Nodes T on E.targetId=T.id;");
		// still need to split potential multiple labels
		const coll = {};
		for (const rec of found[0]) {
			const from = rec.L1.split(",");
			const es = rec.L.split(",");
			const to = rec.L2.split(",");
			// cartesian product
			for (const t of to) {
				for (const f of from) {
					for (const l of es) {
						const key = `${f},${t}`;
						if (coll[key]) {
							if (!_.includes(coll[key], l)) {
								coll[key].push(l);
							}
						} else {
							coll[key] = [l];
						}
					}
				}
			}
		}
		return coll;
	}

	//endregion

	/*
	 * This is the actual API returned to Qwiery
	 * and implements all the adapter methods.
	 * That is, these methods effectively turn graph requests into SQL requests.
	 * */
	const api = {
		//region Node
		/**
		 * Search of the nodes for the given term.
		 * @param term {string} A search term.
		 * @param [fields] {string[]} The properties to consider in the search. If none given the name will be considered only.
		 * @param amount {number} The maximum amount of nodes to return.
		 */
		searchNodes(done) {
			return async ([term, fields, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				if (!amount) {
					amount = 100;
				}
				if (Utils.isEmpty(term)) {
					return done(null, [term, fields], []);
				}
				if (fields.length === 0) {
					fields = ["name"];
				}
				const lowerTerm = term.trim().toLowerCase();
				const whereList:any[] = [];
				const whereOr = {
					[Op.or]: whereList,
				};
				for (const field of fields) {
					const op = {};
					if (field === "labels") {
						op[field] = {
							[Op.substring]: lowerTerm,
						};
					} else {
						op["data." + field] = {
							[Op.substring]: lowerTerm,
						};
					}

					whereList.push(op);
				}
				const records = await N.findAll({ where: whereOr, limit: amount });
				if (records.length > 0) {
					const nodes = records.map((r) => AdapterUtils.dbRecordToNode(r));
					return done(null, [term, fields], nodes);
				} else {
					return done(null, [term, fields], []);
				}
			};
		},

		searchNodesWithLabel(done) {
			return async ([term, fields, label, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				if (!amount) {
					amount = 100;
				}
				if (Utils.isEmpty(term)) {
					return done(null, [term, fields], []);
				}
				if (fields.length === 0) {
					fields = ["name"];
				}
				const lowerTerm = term.trim().toLowerCase();
				const whereList:string[]  = [];
				const whereOr = {
					labels: { [Op.substring]: label },
					[Op.or]: whereList,
				};
				for (const field of fields) {
					const op = {};
					op["data." + field] = {
						[Op.substring]: lowerTerm,
					};
					whereList.push(op);
				}
				const records = await N.findAll({ where: whereOr, limit: amount });
				if (records.length > 0) {
					const nodes = records.map((r) => AdapterUtils.dbRecordToNode(r));
					return done(null, [term, fields], nodes);
				} else {
					return done(null, [term, fields], []);
				}
			};
		},

		createNode(done) {
			return async ([data, id, labels]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}

				const specs = Utils.getNodeSpecs(data, id, labels);
				if (_.isNil(specs)) {
					return done(error.insufficientNodeSpecs(), [data, id, labels], null);
				}
				if (specs.labels.length === 0) {
					specs.labels = [DefaultOptions.defaultNodeLabel];
				}
				if (_.isNil(specs.id)) {
					specs.id = Utils.id();
				}
				// hijack the nodeExist method
				if (await this.nodePresent(specs.id)) {
					throw new Error(`Node with id '${specs.id}' exists already.`);
				}
				try {
					const n = AdapterUtils.nodeSpecsToRecord(specs);
					await N.create(n);
					done(null, [data, id, labels], n);
				} catch (e:any) {
					done(e.message, [data, id, labels], null);
				}
			};
		},

		async nodePresent(id) {
			let present = false;
			await this.nodeExists((x, y, z) => {
				present = z;
			})([id]);
			return present;
		},

		nodeExists(done) {
			return async ([id]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					done(null, [id], nodeExists(id));
				} catch (e:any) {
					error = e.message;
					done(e.message, [id], false);
				}
			};
		},

		createNodes(done) {
			return async ([seq]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const coll:any[] = [];
					for (const item of seq) {
						// only a string or plain object will work here
						const n = await this.createNode(_.noop)([item]);
						if (n) {
							coll.push(n);
						}
					}
					done(null, [seq], coll);
				} catch (e:any) {
					done(e.message, [seq], null);
				}
			};
		},

		updateNode(done) {
			return async ([data, id, labels]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const specs = Utils.getNodeSpecs(data, id, labels);
					if (specs === null) {
						return done(errors.insufficientNodeSpecs(), [data, id, labels], null);
					}
					if (Utils.isEmpty(specs.data)) {
						specs.data = {};
					}
					if (specs.id) {
						specs.data.id = specs.id;
					}
					if (specs.labels) {
						specs.data.labels = specs.labels;
					}

					if (!(await this.nodePresent(specs.data.id))) {
						return done(errors.nodeDoesNotExist(specs.data.id), [data, id, labels], null);
					}
					const found = await N.findOne({ where: { id: specs.id } });
					const rec = AdapterUtils.nodeSpecsToRecord(specs);
					found.labels = rec.labels;
					found.data = rec.data;
					await found.save();
					done(null, [data, id, labels], AdapterUtils.dbRecordToNode(rec));
				} catch (e:any) {
					done(e.message, [data, id, labels], null);
				}
			};
		},

		upsertNode(done) {
			return async ([data, id, labels]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				const specs = Utils.getNodeSpecs(data, id, labels);
				if (specs === null) {
					throw new Error(errors.insufficientNodeSpecs());
				}

				if (specs.id && (await this.nodePresent(specs.id))) {
					const m = this.updateNode(done);
					const ar = [data, id, labels];
					await m(ar);
				} else {
					const m = this.createNode(done);
					const ar = [data, id, labels];
					await m(ar);
				}
			};
		},

		getNode(done) {
			return async ([id]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}

				let sequelizeProjection = {};

				if (_.isString(id)) {
					sequelizeProjection = { where: { id } };
				} else if (_.isFunction(id)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("getNode with a predicate is not supported by the Sqlite adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [id], null);
				} else if (_.isPlainObject(id)) {
					// this is where the Mongo-like specs are turned into Cypher constraints
					try {
						sequelizeProjection = toSequelize(id); //?
					} catch (e:any) {
						return done(e.message, [id], null);
					}
				}

				try {
					const r = await N.findOne(sequelizeProjection);
					if (_.isNil(r)) {
						done(null, [id], null);
					} else {
						done(null, [id], AdapterUtils.dbRecordToNode(r));
					}
				} catch (e:any) {
					done(e.message, [id], null);
				}
			};
		},

		getNodes(done) {
			return async ([projection, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				if (_.isNil(projection)) {
					return done("Nil predicate for getNodes.", [projection, amount], null);
				}
				if (_.isNil(amount)) {
					amount = 1000;
				}

				let sequelizeProjection = {};
				if (_.isFunction(projection)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("getNodes with a function is not supported by the Sqlite adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [projection], null);
				} else if (_.isPlainObject(projection)) {
					try {
						sequelizeProjection = toSequelize(projection);
					} catch (e:any) {
						return done(e.message, [projection, amount], null);
					}
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [projection, amount], null);
				}
				// ensure the given amount
				sequelizeProjection.limit = amount;
				try {
					const records = await N.findAll(sequelizeProjection);
					if (records.length > 0) {
						done(
							null,
							[projection],
							records.map((u) => AdapterUtils.dbRecordToNode(u)),
						);
					} else {
						done(null, [projection, amount], []);
					}
				} catch (e:any) {
					done(e.message, [projection, amount], null);
				}
			};
		},

		getNodesWithLabel(done) {
			return async ([label, amount = 1000]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					done(null, [label, amount], await getNodesWithLabel(label, amount));
				} catch (e:any) {
					done(e.message, [label, amount], null);
				}
			};
		},

		deleteNodes(done) {
			return async ([projection]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};

				if (_.isFunction(projection)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("getEdges with a predicate is not supported by the Neo4j adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [projection], null);
				} else if (_.isPlainObject(projection)) {
					try {
						sequelizeProjection = toSequelize(projection);
					} catch (e:any) {
						return done(e.message, [projection], null);
					}
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [projection], null);
				}
				try {
					const items = await N.findAll(sequelizeProjection);
					items; //?
					for (const item of items) {
						await item.destroy();
					}
					done(null, [projection], []);
				} catch (e:any) {
					done(e.message, [projection], null);
				}
			};
		},

		deleteNode(done) {
			return async ([id]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};
				if (_.isString(id)) {
					sequelizeProjection = { where: { id } };
				} else if (_.isFunction(id)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("deleteNode with a predicate is not supported by the Sqlite adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [id], null);
				} else if (_.isPlainObject(id)) {
					try {
						sequelizeProjection = toSequelize(projection);
					} catch (e:any) {
						return done(e.message, [id], null);
					}
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [id], null);
				}
				try {
					const found = await N.findOne(sequelizeProjection);
					if (!_.isNil(found)) {
						found.destroy();
					}
					done(null, [id], []);
				} catch (e:any) {
					done(e.message, [id], null);
				}
			};
		},

		nodeCount(done) {
			return async ([projection]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};
				if (_.isFunction(projection)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("nodeCount with a predicate is not supported by the Sqlite adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [projection], null);
				} else if (_.isPlainObject(projection)) {
					try {
						sequelizeProjection = toSequelize(projection);
					} catch (e:any) {
						return done(e.message, [projection], null);
					}
				} else if (_.isNil(projection)) {
					sequelizeProjection = {};
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [projection], null);
				}
				try {
					const { count, rows } = await N.findAndCountAll(sequelizeProjection);
					done(null, [projection], count);
				} catch (e:any) {
					done(e.message, [projection], null);
				}
			};
		},

		getNodeLabels(done) {
			return async ([]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const all = getNodeLabels();
					done(null, [], all);
				} catch (e:any) {
					done(e.message, [], null);
				}
			};
		},
		getNodeLabelProperties(done) {
			return async ([labelName]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const labels = await getNodeLabelProperties(labelName);
					done(null, [labelName], labels);
				} catch (e:any) {
					done(e.message, [], null);
				}
			};
		},
		//endregion

		//region Edge
		deleteEdge(done) {
			return async ([id]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};
				if (_.isString(id)) {
					sequelizeProjection = { id };
				} else if (_.isFunction(id)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("deleteEdge with a predicate is not supported by the Neo4j adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [id], null);
				} else if (_.isPlainObject(id)) {
					try {
						sequelizeProjection = toSequelize(id);
					} catch (e:any) {
						return done(e.message, [id], null);
					}
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [id], null);
				}
				try {
					const item = await E.findOne(sequelizeProjection);
					if (!_.isNil(item)) {
						await item.destroy();
					}
					done(null, [id], []);
				} catch (e:any) {
					done(e.message, [id], null);
				}
			};
		},

		getEdge(done) {
			return async ([id]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};
				if (_.isString(id)) {
					sequelizeProjection = { where: { id } };
				} else if (_.isFunction(id)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("getEdge with a predicate is not supported by the Neo4j adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [id], null);
				} else if (_.isPlainObject(id)) {
					// this is where the Mongo-like specs are turned into Cypher constraints
					try {
						sequelizeProjection = toSequelize(id);
					} catch (e:any) {
						return done(e.message, [id], null);
					}
				}
				try {
					const r = await E.findOne(sequelizeProjection);
					if (_.isNil(r)) {
						return done(null, [id], null);
					} else {
						return done(null, [id], AdapterUtils.dbRecordToEdge(r));
					}
				} catch (e:any) {
					done(e.message, [id], null);
				}
			};
		},

		getEdgeBetween(done) {
			return async ([sourceId, targetId]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const found = await E.findOne({ where: { sourceId, targetId } });

					if (!_.isNil(found)) {
						done(null, [sourceId, targetId], found);
					} else {
						done(null, [sourceId, targetId], null);
					}
				} catch (e:any) {
					done(e.message, [sourceId, targetId], null);
				}
			};
		},

		createEdge(done) {
			return async ([sourceId, targetId, data = null, id = null, labels = null]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				const specs = Utils.getEdgeSpecs(sourceId, targetId, data, id, labels);
				if (_.isNil(specs)) {
					return done(error.insufficientEdgeSpecs(), [sourceId, targetId, data, id, labels], null);
				}
				if (specs.labels.length === 0) {
					specs.labels = [DefaultOptions.defaultEdgeLabel];
				}
				if (_.isNil(specs.id)) {
					specs.id = Utils.id();
				}
				if (await edgeExists(specs.id)) {
					throw new Error(`Edge with id '${id}' exists already.`);
				}
				try {
					const e = {
						id: specs.id,
						sourceId: specs.sourceId,
						targetId: specs.targetId,
						labels: specs.labels.join(","),
						data: specs.data,
					};
					await E.create(e);
					done(null, [sourceId, targetId, data, id, labels], e);
				} catch (e:any) {
					done(e.message, [sourceId, targetId, data, id, labels], null);
				}
			};
		},

		upsertEdge(done) {
			return async ([data = null, id = null, labels = null]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const specs = Utils.getEdgeSpecs(data, null, id, labels);
					if (specs === null) {
						return done(errors.insufficientEdgeSpecs(), [data, id, labels], null);
					}
					if (specs.id && (await edgeExists(specs.id))) {
						return this.updateEdge(done)([data, id, labels]);
					} else {
						return this.createEdge(done)([data, null, data, id, labels]);
					}
				} catch (e:any) {
					done(e.message, [data, id, labels], null);
				}
			};
		},

		updateEdge(done) {
			return async ([data, id, labels]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				const specs = Utils.getEdgeSpecs(data, null, id, labels);
				if (_.isNil(specs)) {
					return done(error.insufficientEdgeSpecs(), [data, id, labels], null);
				}
				if (specs.labels.length === 0) {
					specs.labels = [DefaultOptions.defaultEdgeLabel];
				}
				if (_.isNil(specs.id)) {
					specs.id = Utils.id();
				}
				const e = await E.findOne({ where: { id: specs.id } });
				if (!_.isNil(e)) {
					try {
						const newEdge = AdapterUtils.edgeSpecsToRecord(specs);
						e.data = newEdge.data;
						e.labels = newEdge.labels;
						e.sourceId = newEdge.sourceId;
						e.targetId = newEdge.targetId;
						await e.save();
						done(null, [data, id, labels], newEdge);
					} catch (e:any) {
						done(e.message, [data, id, labels], null);
					}
				} else {
					return done(`Edge with id '${specs.id}' does not exist and can not be updated.`);
				}
			};
		},

		getEdgeWithLabel(done) {
			return async ([sourceId, targetId, label]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const found = await E.findOne({ where: { labels: { [Op.substring]: label } } });
					if (!_.isNil(found)) {
						done(null, [sourceId, targetId, label], AdapterUtils.dbRecordToEdge(found));
					} else {
						done(null, [sourceId, targetId, label], null);
					}
				} catch (e:any) {
					done(e.message, [sourceId, targetId, label], null);
				}
			};
		},

		getEdgesWithLabel(done) {
			return async ([label, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					done(null, [label, amount], await getEdgesWithLabel(label, amount));
				} catch (e:any) {
					done(e.message, [label, amount], null);
				}
			};
		},

		getDownstreamEdges(done) {
			return async ([sourceId, amount]) => {
				try {
					if (Utils.isEmpty(amount)) {
						amount = 1000;
					}
					done(null, [sourceId, amount], await getDownstreamEdges(sourceId, amount));
				} catch (e:any) {
					done(e.message, [sourceId, amount], null);
				}
			};
		},

		getUpstreamEdges(done) {
			return async ([targetId, amount]) => {
				try {
					if (Utils.isEmpty(amount)) {
						amount = 1000;
					}
					const records = await E.findAll({ where: { targetId }, limit: amount });
					if (records.length > 0) {
						const edges = records.map((r) => AdapterUtils.dbRecordToEdge(r));
						done(null, [targetId, amount], edges);
					} else {
						done(null, [targetId, amount], []);
					}
				} catch (e:any) {
					done(e.message, [sourceId, amount], null);
				}
			};
		},

		getEdges(done) {
			return async ([projection, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};

				if (_.isFunction(projection)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("getEdges with a predicate is not supported by the Neo4j adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [projection], null);
				} else if (_.isPlainObject(projection)) {
					try {
						sequelizeProjection = toSequelize(projection);
					} catch (e:any) {
						return done(e.message, [projection], null);
					}
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [projection], null);
				}
				try {
					const r = await E.findAll(sequelizeProjection);
					if (r.length > 0) {
						done(
							null,
							[projection],
							r.map((u) => AdapterUtils.dbRecordToEdge(u)),
						);
					} else {
						done(null, [projection], []);
					}
				} catch (e:any) {
					done(e.message, [projection], null);
				}
			};
		},

		edgeExists(done) {
			return async ([id]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const found = await E.findOne({ where: { id } });
					done(null, [id], !_.isNil(found));
				} catch (e:any) {
					error = e.message;
					done(e.message, [id], false);
				}
			};
		},

		edgeCount(done) {
			return async ([projection]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				let sequelizeProjection = {};
				if (_.isFunction(projection)) {
					// kinda possible by looping over all nodes in the db with the predicate but that's not really scalable or good practice
					return done("nodeCount with a predicate is not supported by the Sqlite adapter. Please use Mongo-like projections instead (https://www.mongodb.com/docs/manual/reference/operator/query/).", [projection], null);
				} else if (_.isPlainObject(projection)) {
					try {
						sequelizeProjection = toSequelize(projection);
					} catch (e:any) {
						return done(e.message, [projection], null);
					}
				} else if (_.isNil(projection)) {
					sequelizeProjection = {};
				} else {
					return done("Please use a Mongo-like projections for getNodes, see https://www.mongodb.com/docs/manual/reference/operator/query/.", [projection], null);
				}
				try {
					const { count, rows } = await E.findAndCountAll(sequelizeProjection);
					done(null, [projection], count);
				} catch (e:any) {
					done(e.message, [projection], null);
				}
			};
		},

		getEdgeLabels(done) {
			return async ([]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const found = await E.findAll({
						attributes: ["labels"],
						group: ["labels"],
					}); //?
					const labels = found.map((project) => project.labels);
					const all:string[]  = [];
					for (const label of labels) {
						if (label.indexOf(",") > -1) {
							const parts = label
								.split(",")
								.map((s) => s.trim())
								.filter((s) => s.length > 0);
							all.push(...parts);
						} else {
							all.push(label);
						}
					}
					done(null, [], _.uniq(all));
				} catch (e:any) {
					done(e.message, [], null);
				}
			};
		},
		//endregion

		//region Graph
		/**
		 * Returns the neighborhood of the specified node as a graph.
		 * @param id {string} A node id.
		 * @returns {Promise<Graph>}
		 */
		getNeighborhood(done) {
			return async ([id, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const g = new Graph();
					const addNode = async (id) => {
						if (!g.nodeIdExists(id)) {
							const node = await getNodeById(id);
							g.addNode(node);
						}
					};
					const downAmount = Math.floor(amount / 2);
					const upAmount = amount - downAmount;
					const downEdges = await getDownstreamEdges(id, downAmount);
					for (const e of downEdges) {
						await addNode(e.sourceId);
						await addNode(e.targetId);
						g.addEdge(e);
					}
					const upEdges = await getUpstreamEdges(id, upAmount);
					for (const e of upEdges) {
						await addNode(e.sourceId);
						await addNode(e.targetId);
						g.addEdge(e);
					}

					done(null, [id, amount], g);
				} catch (e:any) {
					done(e.message, [id, amount], null);
				}
			};
		},

		clear(done) {
			return async () => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					await E.destroy({
						where: {},
						truncate: true,
					});
					await N.destroy({
						where: {},
						truncate: true,
					});
					done(null, [], "created");
				} catch (e:any) {
					done(e.message, [], null);
				}
			};
		},

		/** @inheritdoc */
		inferSchemaGraph(done) {
			return async ([cached]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}
				try {
					const dic = await getSchemaRecords();
					// add the nodes
					const labelMap = {};
					const g = new Graph();
					let u, v;
					_.forEach(dic, (v, k) => {
						[u, v] = k.split(",");
						if (!labelMap[u]) {
							labelMap[u] = g.addNode(u);
						}
						if (!labelMap[v]) {
							labelMap[v] = g.addNode(v);
						}
					});
					for (const key of Object.keys(dic)) {
						const [u, v] = key.split(",");
						for (const edgeLabel of dic[key]) {
							const source = labelMap[u];
							const target = labelMap[v];
							g.addEdge({ sourceId: source.id, targetId: target.id, name: edgeLabel });
						}
					}
					done(null, [], g);
				} catch (e:any) {
					done(e.message, [], null);
				}
			};
		},

		/**
		 * A path query defines a patter, e.g. ["A",*,"B","knows","C"].
		 * There are only two possibilities:
		 * - an arbitrary edge, meaning all nodes with the label in the next entry
		 * - a specific edge label, the next item has to be *
		 * @param path
		 * @return {Promise<Graph>}
		 */
		pathQuery(done) {
			return async ([path, amount]) => {
				if (!isInitialized) {
					await setup(options[AdapterId]);
				}

				try {
					const found = await pathQuery(path, amount);
					done(null, [path], found);
				} catch (e:any) {
					done(e.message, [], null);
				}
			};
		},
		loadGraph(done) {
			return async ([name]) => {
				// todo: how to transfer the data from one database to another via Sequelize?
				throw new Error("Food for thought.");
				// if (!isInitialized) {
				// 	await setup(options[AdapterId]);
				// }
				//
				// try {
				// 	done(null, [], null);
				// } catch (e:any) {
				// 	done(e.message, [], null);
				// }
			};
		},
		//endregion
	};

	/**
	 * Sets the schema up.
	 * @param opt {*} The options specific for this adapter.
	 * @return {Promise<void>}
	 */
	async function setup(options = {}) {
		if (isInitialized) {
			return;
		}
		error = null;
		try {
			const schema = await setupSqliteSchema(options);
			db = schema.db;
			N = schema.Node;
			E = schema.Edge;
		} catch (e:any) {
			console.error("Very likely that you have not included the sqlite3 package in your solution.");
			error = e.message;
		}
		isInitialized = true;
	}

	process.nextTick(() => {
		done(null, api);
	});
}
